import glob
import asyncio

from pathlib import Path
from datetime import datetime

from lib.utils.sjob_manager import SlurmJobManager
from tests.utils.mock_sjob_manager import MockSlurmJobManager

from lib.realms.smartseq3.utils.ss3_utils import SS3Utils

from lib.realms.smartseq3.utils.sample_file_handler import SampleFileHandler
from lib.realms.smartseq3.report.report_generator import Smartseq3ReportGenerator

from lib.utils.logging_utils import custom_logger
from lib.utils.branch_template import RealmTemplate
from lib.utils.destiny_interface import DestinyInterface
from lib.utils.config_loader import ConfigLoader
from lib.utils.slurm_utils import generate_slurm_script
from lib.realms.smartseq3.utils.yaml_utils import write_yaml


DEBUG = False
logging = custom_logger("SmartSeq3")

class SmartSeq3(DestinyInterface, RealmTemplate):
    # Class variables
    config = ConfigLoader().load_config("ss3_config.json")

    def __init__(self, doc):
        self.doc = doc
        self.proceed = self._check_required_fields()

        # TODO: What if I return None if not self.proceed?
        if self.proceed:
            self.project_info = self._extract_project_info()

            self.project_dir = self.ensure_project_directory()
            self.project_info['project_dir'] = self.project_dir

            self.samples = []


    def _extract_project_info(self):
        """
        Extracts project information from the provided document.

        :return: A dictionary containing selected project information or an empty dictionary in case of an error.
        """
        try:
            project_info = {
                "project_name": self.doc.get('project_name', '').replace(".", "__"),
                "project_id": self.doc.get('project_id'),
                "escg_id": self.doc.get('customer_project_reference'),
                "library_prep_option": self.doc.get('details', {}).get('library_prep_option'),
                "contact": self.doc.get('contact'),  # Is this an email or a name?
                "ref_genome": self.doc.get('reference_genome'),
                "sequencing_setup": self.doc.get('details', {}).get('sequencing_setup'),
            }

            return project_info

        except Exception as e:
            logging.error(f"Error occurred while extracting project information: {e}")
            return {}  # Return an empty dict or some default values to allow continuation

    def _check_required_fields(self):
        required_fields = self.config.get("required_fields", [])
        sample_required_fields = self.config.get("sample_required_fields", [])

        missing_keys = [field for field in required_fields if not self._is_field(field, self.doc)]
        
        if missing_keys:
            logging.warning(f"Missing required project information: {missing_keys}.")
            return False

        # Check sample-specific required fields
        samples = self.doc.get('samples', {})
        for sample_id, sample_data in samples.items():
            for field in sample_required_fields:
                if not self._is_field(field, sample_data):
                    logging.warning(f"Missing required sample information '{field}' in sample '{sample_id}'.")

                    if "total_reads_(m)" in field:
                        # TODO: Send this message as a notification on Slack
                        logging.warning("Consider running 'Aggregate Reads' in LIMS.")

                    return False
                
        return True

    def _is_field(self, field_path, data):
        """Check if a nested field exists in a dictionary."""
        keys = field_path.split('.')
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return False
        return True

    def ensure_project_directory(self):
        """
        Ensures that the project directory exists.
        Returns the Path object of the directory if successful, or None if an error occurs.
        """
        try:
            project_dir = Path(self.config['smartseq3_dir']) / 'projects' / self.project_info['project_name']
            project_dir.mkdir(parents=True, exist_ok=True)
            return project_dir
        except Exception as e:
            logging.error(f"Failed to create project directory: {e}")
            return None


    async def process(self):
        self.status = "processing"
        print("Processing SmartSeq3 project")
        self.samples = self.extract_samples()
        if not self.samples:
            logging.warning("No samples found for processing. Returning...")
            return
        tasks = [sample.process() for sample in self.samples]
        print(f"Sample tasks created. Waiting for completion...: {tasks}")
        await asyncio.gather(*tasks)
        print("All samples processed. Finalizing project...")
        self.finalize_project(self.samples)

    def extract_samples(self):
        samples = []

        for sample_id, sample_data in self.doc.get('samples', {}).items():
            sample = SS3Sample(sample_id, sample_data, self.project_info, self.config)

            if sample.flowcell_id:
                samples.append(sample)
            else:
                logging.warning(f"Skipping {sample_id}. No flowcell IDs found.")

        return samples

    def finalize_project(self, samples):
        # Logic to gather results and prepare for delivery
        pass
        


    def pre_process(self, doc):
        pass

    def create_slurm_job(self, sample):
        # try:
        #     output_file = f"sim_out/10x/{sample['scilife_name']}_slurm_script.sh"
        #     # Use your method to generate the Slurm script here
        #     generate_slurm_script(sample, "sim_out/10x/slurm_template.sh", output_file)
        # except Exception as e:
        #     logging.warning(f"Error in creating Slurm job for sample {sample['scilife_name']}: {e}")
        pass

    def submit_job(self, script):
        """
        Submits a job to Slurm. This uses the JobManager's functionality.
        """
        # Use JobManager to submit the job
        return super().submit_job(script)

    def monitor_job(self, job_id):
        """
        Monitors the submitted Slurm job. This uses the JobManager's functionality.
        """
        # Use JobManager to monitor the job
        return super().monitor_job(job_id)
    
    def post_process(self, result):
        """
        Concrete implementation of the post_process method.
        """
        pass



class SS3Sample():
    def __init__(self, sample_id, sample_data, project_info, config):
        # TODO: self.id must be demanded by a template class
        self.id = sample_id
        self.sample_data = sample_data
        self.project_info = project_info

        # Initialize barcode
        self.barcode = self.get_barcode()

        # Collect barcode // Any TODO s here? Probably do checks / add to separate `get_barcode` method?
        # self.barcode = self.sample_data['library_prep']['A'].get('barcode', '').split('-')[-1]

        # Collect flowcell ID
        self.flowcell_id = self._get_latest_flowcell()

        self.config = config
        # self.job_id = None
        self.status = "pending"  # other statuses: "processing", "completed", "failed"
        self.metadata = None

        # Define the parent project directory
        self.project_dir = self.project_info.get('project_dir')

        # Define the sample directory
        # NOTE: This directory is not created yet. To be verified in the post_process method.
        self.sample_dir = self.project_dir / self.id

        if DEBUG:
            self.sjob_manager = MockSlurmJobManager()
        else:
            self.sjob_manager = SlurmJobManager()

        # Initialize SampleFileHandler
        self.file_handler = SampleFileHandler(self)

    async def process(self):
        # Collect metadata for this sample
        logging.debug(f"Processing sample {self.id}")
        yaml_metadata = self._collect_yaml_metadata()
        if not yaml_metadata:
            logging.warning(f"Metadata missing for sample {self.id}")
            return None

        logging.debug("Metadata collected. Creating YAML file")

        self.create_yaml_file(yaml_metadata)

        # TODO: Check if the YAML file was created successfully
        logging.debug("YAML file created.")

        logging.debug("Creating Slurm script")
        slurm_metadata = self._collect_slurm_metadata()
        if not slurm_metadata:
            logging.warning(f"Slurm metadata missing for sample {self.id}")
            return None

        # Create Slurm script and submit job
        slurm_template_path = self.config['slurm_template']
        if not generate_slurm_script(slurm_metadata, slurm_template_path, self.file_handler.slurm_script_path):
            logging.error(f"Failed to create Slurm script for sample {self.id}.")
            return None

        # Submit the job
        logging.debug("Slurm script created. Submitting job")
        self.job_id = await self.sjob_manager.submit_job(self.file_handler.slurm_script_path)

        # if self.job_id:
        #     logging.debug(f"[{self.id}] Job submitted with ID: {self.job_id}")
            
        #     asyncio.create_task(self.sjob_manager.monitor_job(self.job_id, self))
        #     logging.debug(f"[{self.id}] Job {self.job_id} submitted for monitoring.")
        # else:
        #     logging.error(f"[{self.id}] Failed to submit job.")
        #     return None
        
        if self.job_id:
            logging.debug(f"[{self.id}] Job submitted with ID: {self.job_id}")
            # Wait here for the monitoring to complete before exiting the process method
            await self.sjob_manager.monitor_job(self.job_id, self)
            logging.debug(f"[{self.id}] Job {self.job_id} monitoring complete.")
        else:
            logging.error(f"[{self.id}] Failed to submit job.")
            return None

        # # Monitor the job
        # if self.job_id:
        #     await self.sjob_manager.monitor_job(self.job_id)

        # Monitor job asynchronously
        # asyncio.create_task(self.sjob_manager.monitor_job(self.job_id, self.check_status))
        

        # Perform any necessary post-processing
        # self.post_process()

    # TODO: Assess if this should be part of the SlurmJobManager class and move it there
    # def check_status(self, job_id, status):
    #     print(f"Job {job_id} status: {status}")
    #     if status == "COMPLETED":
    #         print(f"Sample {self.id} processing completed.")
    #         self.post_process()
    #         self.status = "completed"
    #     elif status in ["FAILED", "CANCELLED"]:
    #         self.status = "failed"
    #         print(f"Sample {self.id} processing failed.")


    def get_barcode(self):
        """ Retrieve and validate the barcode from sample data. """
        barcode = self.sample_data['library_prep']['A'].get('barcode', None)
        if barcode:
            return barcode.split('-')[-1]
        else:
            logging.warning(f"No barcode found in StatusDB for sample {self.id}.")
            return None  # Or handle more appropriately based on your application's requirements

    def _collect_yaml_metadata(self):
        # TODO: Replace this with get_barcode() and no need do this here, do it in __init__
        # barcode = self.sample_data['library_prep']['A'].get('barcode', '').split('-')[-1]
        
        
        # bc_path = f"{self.config['smartseq3_dir']}/barcodes/{barcode}.txt"

        # TODO: If barcode does not exist, make barcode using mkbarcode
        # if not Path(bc_path).exists():
        #     logging.warning(f"Barcode {barcode} not found at {bc_path}.")
        #     return None
        
        # seq_root = self.config["seq_root_dir"]

        # NOTE: zUMIs does not support multiple flowcells per sample
        # Potential solutions:
        #   1. SmartSeq3 sample libraries should not be sequenced across multiple flowcells
        #       SmartSeq3 libraries should not be re-sequenced in the same project
        #   2. Merge fastq files from multiple flowcells

        # Select the latest flowcell for analysis
        if self.flowcell_id:
            fastqs = self.file_handler.locate_fastq_files()
            if fastqs is None:
                logging.warning(f"No FASTQ files found for sample '{self.id}' in flowcell '{self.flowcell_id}'. Ensure files are correctly named and located.")
                return None
        else:
            logging.warning(f"No flowcell found for sample {self.id}")
            return None

        # if not all(fastqs.values()):
        #     logging.warning(f"Not all fastq files found at {fastq_path}")
        #     return None
        
        seq_setup = self.project_info.get('sequencing_setup', '')
        if seq_setup:
            # read_setup = self._transform_seq_setup(seq_setup)
            read_setup = SS3Utils.transform_seq_setup(seq_setup)

        ref_gen = self.project_info.get('ref_genome', '')
        # NOTE: Might break if the reference genome format is odd.
        # TODO: Might need to make more robust or even map the ref genomes to their paths
        # idx_path, gtf_path = self._get_ref_paths(ref_gen, self.config)

        # if idx_path and gtf_path:
        #     ref_paths = {
        #         'gen_path': idx_path,
        #         'gtf_path': gtf_path
        #     }
        # else:
        #     return None  # or handle the missing reference paths appropriately

        ref_paths = self.file_handler.locate_ref_paths(ref_gen)

        if self.barcode is None:
            logging.warning(f"Barcode not available for sample {self.id}")
            return None
        
        if not self.file_handler.ensure_barcode_file():
            logging.error(f"Failed to create barcode file for sample {self.id}. Skipping...")
            return None

        try:
            metadata = {
                # 'plate': self.id, # NOTE: Temporarily not used, but might be used when we name everything after ngi
                'plate': self.sample_data.get('customer_name', ''),
                'barcode': self.barcode,
                'bc_file': self.file_handler.barcode_fpath,
                'fastqs': {k: str(v) for k, v in fastqs.items() if v},
                'read_setup': read_setup,
                'ref': ref_paths,
                'outdir': str(self.sample_dir),
                'out_yaml': self.project_dir / f"{self.id}.yaml"
            }
        except Exception as e:
            logging.error(f"Error constructing metadata for sample {self.id}: {e}")
            return None    

        self.metadata = metadata

        return metadata
    

    def _get_latest_flowcell(self):
        """
        Selects the latest flowcell for the current sample.

        Returns:
            The latest flowcell ID or None if no valid flowcells are found.
        """
        try:
            latest_fc = None
            latest_date = None
            if 'library_prep' in self.sample_data:
                for prep_info in self.sample_data['library_prep'].values():
                    for fc_id in prep_info.get('sequenced_fc', []):
                        # fc_date = self._parse_fc_date(fc_id)
                        fc_date = SS3Utils.parse_fc_date(fc_id)
                        if fc_date and (not latest_date or fc_date > latest_date):
                            latest_date = fc_date
                            latest_fc = fc_id

            if not latest_fc:
                logging.warning(f"No valid flowcells found for sample {self.id}.")
            return latest_fc

        except Exception as e:
            logging.error(f"Error extracting latest flowcell info for sample '{self.id}': {e}", exc_info=True)
            return None


    def _parse_fc_date(self, flowcell_id):
        """
        Parses the date from a flowcell ID, considering different date formats.

        Args:
            flowcell_id: The flowcell ID to parse.

        Returns:
            A date object representing the date of the flowcell or None if parsing fails.
        """
        date_formats = ['%Y%m%d', '%y%m%d']  # Potential date formats
        date_str = flowcell_id.split('_')[0]

        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        # Log an error if all parsing attempts fail
        logging.error(f"Could not parse date for flowcell {flowcell_id}.", exc_info=True)
        return None
    
    def _collect_slurm_metadata(self):
        # TODO: The project directory is checked and created by ensure_project_directory(). This might be redundant.
        # project_dir = Path(self.config['smartseq3_dir']) / 'projects' / self.project_info['project_name']
        # project_dir.mkdir(exist_ok=True)

        try:
            metadata = {
                'project_name': self.project_info['project_name'],
                'project_dir': self.project_dir,
                # 'sample_id': self.id, # Temporarily not used, but might be used when we name everything after ngi
                'plate_id': self.id, # self.sample_data.get('customer_name', ''),
                'yaml_settings_path': self.project_dir / f"{self.id}.yaml",
                'zumis_path': self.config['zumis_path'],
            }
        except Exception as e:
            logging.error(f"Error constructing metadata for sample {self.id}: {e}")
            return None    

        return metadata

    def _transform_seq_setup(self, seq_setup_str):
        """
        Transforms a sequencing setup string into a detailed format for each read type.

        Args:
            seq_setup_str (str): Sequencing setup string in the format "R1-I1-I2-R2".

        Returns:
            dict: A dictionary with formatted strings for each read type.
        """
        r1, i1, i2, r2 = seq_setup_str.split('-')

        return {
            'R1': (f"cDNA(23-{r1})", "UMI(12-19)"),
            'R2': f"cDNA(1-{r2})",
            'I1': f"BC(1-{i1})",
            'I2': f"BC(1-{i2})"
        }

    def _get_ref_paths(self, ref_gen, config):
        """
        Maps a reference genome to its STAR index and GTF file paths.

        Args:
            ref_gen (str): Reference genome string, e.g., "Zebrafish (Danio rerio, GRCz10)".
            config (dict): Configuration object containing the mapping.

        Returns:
            tuple: A tuple containing the STAR index path and GTF file path, or None if not found.
        """
        try:
            # Extract species name before the first comma
            species_key = ref_gen.split(',')[0].split("(")[1].strip().lower()
            idx_path = config['gen_refs'][species_key]['idx_path']
            gtf_path = config['gen_refs'][species_key]['gtf_path']
            return idx_path, gtf_path
        except KeyError as e:
            logging.warning(f"Reference for {e} species not found in config. Handle {self.id} manually.")
            return None, None


    def create_yaml_file(self, metadata):
        write_yaml(self.config, metadata)


    def post_process(self):
        print(f"Post-processing sample {self.id}...")
        # print(self.config)
        # print(self.project_info)
        # print(self.sample_data)
        # print(self.metadata)
        # print(self.flowcell_id)

        # Check if sample output is valid
        if not self.file_handler.is_output_valid():
            # TODO: Send a notification (Slack?) for manual intervention
            logging.error(f"Sample {self.id} output is invalid. Skipping post-processing.")
            return

        self.file_handler.create_directories()

        # Instantiate report generator
        report_generator = Smartseq3ReportGenerator(self)

        # Collect stats
        if not report_generator.collect_stats():
            logging.error(f"Error collecting stats for sample {self.id}. Skipping report generation.")
            return

        # Create Plots
        if not report_generator.create_graphs():
            logging.error(f"Error creating plots for sample {self.id}. Skipping report generation.")
            return

        # Generate Report
        report_generator.render(format='PDF')