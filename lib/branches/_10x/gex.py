import asyncio
import logging

# from lib.branches.branch_utils.genome_mapper import GenomeMapper
# from ygg_trunk import DEBUG

from lib.utils.sjob_manager import SlurmJobManager
from tests.utils.mock_sjob_manager import MockSlurmJobManager
from lib.utils.slurm_utils import generate_slurm_script
from lib.branches._10x.utils._10x_utils import compile_metadata

DEBUG = True
# def process(doc):    
#     # mapper = GenomeMapper(_10x_config["species_mappings"])
#     try:
#         # samples = collect_meta_per_sample(doc)
#         # return samples
#         # for sample_name, sample in samples.items():
#             # output_file = f"sim_out/10x/{sample_name}_slurm_script.sh"
#             # generate_slurm_script(sample, "sim_out/10x/slurm_template.sh", output_file)


#     except KeyError as e:
#         logging.warning(f"GEX: Error while processing couchDB data: {e}")
#     pass


class GEXProject:
    def __init__(self, project_info, config):
        self.project_info = project_info
        self.config = config

    async def process(self, doc):
        # GEX-specific logic
        samples = self.extract_samples(doc)
        print("Samples extracted. Processing...")
        tasks = [sample.process() for sample in samples]
        print(f"Sample tasks created. Waiting for completion...: {tasks}")
        await asyncio.gather(*tasks)
        self.finalize_project(samples)

    def extract_samples(self, doc):
        """
        Extracts samples from the provided document and creates Sample instances.

        :param doc: The document containing sample information.
        :return: A list of Sample instances.
        """
        samples = []
        for sample_id, sample_data in doc.get('samples', []).items():
            # Assuming 'samples' is a list of dictionaries, each representing a sample
            # You may need to adjust this based on your actual document structure
            sample = GEXSample(sample_id, sample_data, self.project_info, self.config)
            samples.append(sample)
        return samples

    def finalize_project(self, samples):
        # Logic to gather results and prepare for delivery
        pass


class GEXSample:
    def __init__(self, sample_id, metadata, project_info, config):
        # TODO: self.id must be demanded by a template class
        self.id = sample_id
        self.metadata = metadata
        self.project_info = project_info
        self.config = config
        self.status = "pending"  # other statuses: "processing", "completed", "failed"

        if DEBUG:
            self.sjob_manager = MockSlurmJobManager()
        else:
            self.sjob_manager = SlurmJobManager()

        self.proceed = self._check_validity()

    def _check_validity(self):
        # TODO: Implement checks to determine if the sample is valid for processing
        return True  # or False based on the checks

    async def process(self):
        # Pre-processing
        self.status = "processing"

        print(f"Processing sample: {self.id}")

        # print(self.project_info['library_prep_option'], self.project_info['ref_genome'])
        slurm_data = compile_metadata(self.metadata, self.id, self.project_info, self.config)
        output_file = f"sim_out/10x/{self.id}_slurm_script.sh"
        # Submit Slurm job asynchronously
        script_path = generate_slurm_script(slurm_data, "templates/10x/slurm_template.sh", output_file)
        print(f"Slurm script generated.")
        job_id = await self.sjob_manager.submit_job(script_path)
        print(f"Job submitted with ID: {job_id}")
        # Monitor job asynchronously
        # asyncio.create_task(self.sjob_manager.monitor_job(job_id, self.check_status))
        asyncio.create_task(self.sjob_manager.monitor_job(job_id, self))
        # await self.sjob_manager.monitor_job(job_id)
        print(f"Job {job_id} submitted for monitoring.")
        # self.post_process()
        # self.status = "completed"


    # # TODO: Assess if this should be part of the SlurmJobManager class and move it there
    # def check_status(self, job_id, status):
    #     print(f"Job {job_id} status: {status}")
    #     if status == "COMPLETED":
    #         print(f"Sample {self.id} processing completed.")
    #         self.post_process()
    #         self.status = "completed"
    #     elif status in ["FAILED", "CANCELLED"]:
    #         self.status = "failed"
    #         print(f"Sample {self.id} processing failed.")


    def post_process(self):
        # Post-processing logic
        print(f"Sample {self.id} post-processing...")

    # Additional methods for QC, path association, etc.