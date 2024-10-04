import csv
from typing import Any, Dict, List, Optional

from lib.base.abstract_sample import AbstractSample
from lib.core_utils.logging_utils import custom_logger
from lib.module_utils.sjob_manager import SlurmJobManager
from lib.module_utils.slurm_utils import generate_slurm_script
from lib.realms.tenx.utils.sample_file_handler import SampleFileHandler
from lib.realms.tenx.utils.tenx_utils import TenXUtils

logging = custom_logger(__name__.split(".")[-1])

DEBUG: bool = True  # Set to False in production


class TenXRunSample(AbstractSample):
    """Class representing a TenX run sample."""

    def __init__(
        self,
        sample_id: str,
        lab_samples: List[Any],
        project_info: Dict[str, Any],
        config: Dict[str, Any],
        yggdrasil_db_manager: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize a TenXRunSample instance.

        Args:
            sample_id (str): The run sample ID.
            lab_samples (List[Any]): A list of lab sample instances.
            project_info (Dict[str, Any]): Project-specific information.
            config (Dict[str, Any]): Configuration data.
            yggdrasil_db_manager (Any): Yggdrasil database manager instance.
            **kwargs (Any): Additional keyword arguments.
        """
        self.run_sample_id: str = sample_id
        self.lab_samples: List[Any] = lab_samples
        self.project_info: Dict[str, Any] = project_info
        self.config: Dict[str, Any] = config
        self.ydm: Any = yggdrasil_db_manager

        # self.decision_table = TenXUtils.load_decision_table("10x_decision_table.json")
        self.feature_to_library_type: Dict[str, Any] = self.config.get(
            "feature_to_library_type", {}
        )
        self._status: str = "initialized"
        self.file_handler: SampleFileHandler = SampleFileHandler(self)

        self.features: List[str] = self._collect_features()
        self.pipeline_info: Optional[Dict[str, Any]] = self._get_pipeline_info()
        self.reference_genomes: Optional[Dict[str, str]] = (
            self.collect_reference_genomes()
        )

        if DEBUG:
            # Use a mock SlurmJobManager for debugging purposes
            from tests.mocks.mock_sjob_manager import MockSlurmJobManager

            self.sjob_manager: SlurmJobManager = MockSlurmJobManager()
        else:
            self.sjob_manager = SlurmJobManager()

    @property
    def id(self) -> str:
        """Get the run sample ID.

        Returns:
            str: The run sample ID.
        """
        return self.run_sample_id

    @property
    def status(self) -> str:
        """Get the current status of the sample.

        Returns:
            str: The current status.
        """
        return self._status

    @status.setter
    def status(self, value: str) -> None:
        """Set the current status of the sample.

        Args:
            value (str): The new status value.
        """
        self._status = value

    def collect_reference_genomes(self) -> Optional[Dict[str, str]]:
        """Collect reference genomes from lab samples and ensure consistency.

        Returns:
            Optional[Dict[str, str]]: A dictionary mapping reference keys to genome paths,
                or None if an error occurs.
        """
        ref_genomes: Dict[str, str] = {}
        feature_to_ref_key = self.config.get("feature_to_ref_key", {})

        for lab_sample in self.lab_samples:
            if lab_sample.reference_genome:
                ref_key = feature_to_ref_key.get(lab_sample.feature)
                if not ref_key:
                    logging.error(
                        f"Feature '{lab_sample.feature}' is not recognized for reference genome mapping."
                    )
                    continue

                # TODO: test this logic - if existing ref same as another ref in lab sample, keep one e.g. take the set. Why fail this?
                # Ensure no conflicting reference genomes for the same ref_key
                existing_ref = ref_genomes.get(ref_key)
                if existing_ref and existing_ref != lab_sample.reference_genome:
                    logging.debug(
                        f"Existing reference genome: {existing_ref} != {lab_sample.reference_genome}"
                    )
                    logging.error(
                        f"Conflicting reference genomes found for reference key '{ref_key}' "
                        f"in sample '{self.run_sample_id}'"
                    )
                    self.status = "failed"
                    return None
                else:
                    ref_genomes[ref_key] = lab_sample.reference_genome
            else:
                logging.error(
                    f"Lab sample {lab_sample.lab_sample_id} is missing a reference genome."
                )
                self.status = "failed"
                return None
        return ref_genomes

    def _get_pipeline_info(self) -> Optional[Dict[str, Any]]:
        """Get the pipeline information for the sample.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing pipeline information,
                or None if not found.
        """
        library_prep_method = self.project_info.get("library_prep_method", "")
        return TenXUtils.get_pipeline_info(library_prep_method, self.features)

    def _collect_features(self) -> List[str]:
        """Collect features from lab samples.

        Returns:
            List[str]: A list of unique features.
        """
        features = [lab_sample.feature for lab_sample in self.lab_samples]
        return list(set(features))

    async def process(self):
        """Process the sample."""
        logging.info("\n")
        logging.info(f"[{self.run_sample_id}] Processing...")

        # Step 1: Verify that all subsamples have FASTQ files
        # TODO: Also check any other requirements
        missing_fq_labsamples = [
            lab_sample.lab_sample_id
            for lab_sample in self.lab_samples
            if not lab_sample.fastq_dirs
        ]
        if missing_fq_labsamples:
            logging.error(
                f"[{self.run_sample_id}] Missing FASTQ files for lab-samples: "
                f"{missing_fq_labsamples}. Skipping..."
            )
            self.status = "failed"
            return

        # Step 2: Determine the pipeline and additional files required
        if not self.pipeline_info:
            logging.error(
                f"[{self.run_sample_id}] No pipeline information found. Skipping..."
            )
            self.status = "failed"
            return

        pipeline = self.pipeline_info.get("pipeline", "")
        pipeline_exec = self.pipeline_info.get("pipeline_exec", "")

        logging.debug(f"[{self.run_sample_id}] Pipeline: {pipeline}")
        logging.debug(f"[{self.run_sample_id}] Pipeline executable: {pipeline_exec}")

        logging.info(f"[{self.run_sample_id}] Generating required files...")
        # Step 3: Generate necessary files based on pipeline requirements
        # TODO: Register generated files in the file handler
        if self.pipeline_info.get("libraries_csv"):
            self.generate_libraries_csv()
        if self.pipeline_info.get("feature_ref"):
            self.generate_feature_reference_csv()
        if self.pipeline_info.get("multi_csv"):
            self.generate_multi_sample_csv()

        cellranger_command = self.assemble_cellranger_command()

        slurm_metadata = {
            "sample_id": self.run_sample_id,
            "project_name": self.project_info.get("project_name", ""),
            "output_dir": str(self.file_handler.sample_dir),
            "cellranger_command": cellranger_command,
        }

        # logging.debug(f"Slurm metadata: {slurm_metadata}")

        slurm_template_path = self.config.get("slurm_template", "")
        if not generate_slurm_script(
            slurm_metadata, slurm_template_path, self.file_handler.slurm_script_path
        ):
            logging.error(f"[{self.run_sample_id}] Failed to generate SLURM script.")
            return None

        # Step 4: Submit the SLURM script
        if not self.pipeline_info.get("submit", False):
            logging.info(
                f"[{self.run_sample_id}] According to decision table, we should not submit. "
                f"Handle manually!"
            )
            return
        logging.debug(f"[{self.run_sample_id}] Slurm script created. Submitting job...")
        self.status = "processing"
        self.job_id: Optional[str] = await self.sjob_manager.submit_job(
            self.file_handler.slurm_script_path
        )

        if self.job_id:
            logging.debug(
                f"[{self.run_sample_id}] Job submitted with ID: {self.job_id}"
            )
            # Wait here for the monitoring to complete before exiting the process method
            await self.sjob_manager.monitor_job(self.job_id, self)
            logging.debug(
                f"[{self.run_sample_id}] Job {self.job_id} monitoring complete."
            )
        else:
            logging.error(f"[{self.run_sample_id}] Failed to submit job.")
            self.status = "failed"
            return None

    def assemble_cellranger_command(self) -> str:
        """Assemble the Cell Ranger command based on the pipeline information.

        Returns:
            str: The assembled command string ready to be executed.
        """
        if self.pipeline_info is None:
            raise ValueError("Pipeline information is missing.")

        if self.reference_genomes is None:
            raise ValueError("Reference genomes information is missing.")

        command_parts = [
            f"{self.pipeline_info['pipeline_exec']} {self.pipeline_info['pipeline']}",
        ]

        required_args = self.pipeline_info.get("required_arguments", [])
        additional_args = self.pipeline_info.get("fixed_arguments", [])

        # Mapping of argument names to their values
        arg_values: Dict[str, Any] = {
            "--id": self.run_sample_id,
            # '--transcriptome': self.config.get('gene_expression_reference'),
            "--fastqs": ",".join(
                [",".join(paths) for paths in self.lab_samples[0].fastq_dirs.values()]
            ),
            "--sample": self.lab_samples[0].lab_sample_id,
            "--libraries": str(
                self.file_handler.base_dir / f"{self.run_sample_id}_libraries.csv"
            ),
            "--feature-ref": str(
                self.file_handler.base_dir
                / f"{self.run_sample_id}_feature_reference.csv"
            ),
            "--csv": str(
                self.file_handler.base_dir / f"{self.run_sample_id}_multi.csv"
            ),
        }

        # Add references based on the pipeline
        if self.pipeline_info.get("pipeline") == "count":
            if "gex" in self.reference_genomes:
                arg_values["--transcriptome"] = self.reference_genomes["gex"]
        elif self.pipeline_info.get("pipeline") == "vdj":
            if "vdj" in self.reference_genomes:
                arg_values["--reference"] = self.reference_genomes["vdj"]
        elif self.pipeline_info.get("pipeline") == "atac":
            if "atac" in self.reference_genomes:
                arg_values["--reference"] = self.reference_genomes["atac"]
        elif self.pipeline_info.get("pipeline") == "multi":
            # references are specified in the multi-sample CSV file
            pass

        for arg in required_args:
            value = arg_values.get(arg)
            if value:
                command_parts.append(f"{arg}={value}")

        # Include additional arguments
        command_parts.extend(additional_args)

        # Join all parts into a single command string
        command = " \\\n    ".join(command_parts)
        return command

    def generate_libraries_csv(self) -> None:
        """Generate the libraries CSV file required for processing."""
        logging.info(f"[{self.run_sample_id}] Generating library CSV")
        library_csv_path = (
            self.file_handler.base_dir / f"{self.run_sample_id}_libraries.csv"
        )

        with open(library_csv_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=["fastqs", "sample", "library_type"]
            )
            writer.writeheader()
            for lab_sample in self.lab_samples:
                feature_type = self.feature_to_library_type.get(lab_sample.feature)
                if not feature_type:
                    logging.error(
                        f"[{self.run_sample_id}] Feature type not found for feature "
                        f"'{lab_sample.feature}' in sample '{lab_sample.sample_id}'"
                    )
                    continue
                # Write one row per FASTQ directory
                for paths in lab_sample.fastq_dirs.values():
                    for path in paths:
                        writer.writerow(
                            {
                                "fastqs": str(path),
                                "sample": lab_sample.lab_sample_id,
                                "library_type": feature_type,
                            }
                        )

    def generate_feature_reference_csv(self) -> None:
        """Generate the feature reference CSV file required for processing."""
        logging.info(f"[{self.run_sample_id}] Generating feature reference CSV")
        pass

    def generate_multi_sample_csv(self) -> None:
        """Generate the multi-sample CSV file required for processing."""
        logging.info(f"[{self.run_sample_id}] Generating multi-sample CSV")
        pass

    def post_process(self) -> None:
        """Perform post-processing steps after job completion."""
        logging.info(f"[{self.run_sample_id}] Post-processing...")
        pass
