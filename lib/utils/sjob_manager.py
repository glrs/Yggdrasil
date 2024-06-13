import re
import asyncio
import subprocess

from lib.utils.config_loader import configs
from lib.utils.logging_utils import custom_logger

logging = custom_logger(__name__.split('.')[-1])

# import asyncio
# import logging
# import re

# from pathlib import Path

# from lib.utils.config_loader import configs

# class SlurmJobManager:
#     def __init__(self, polling_interval=1.0, command_timeout=8.0):
#         self.polling_interval = polling_interval
#         self.command_timeout = command_timeout

#         # TODO: Make sure the path to the slurm_manager.sh script exists or log an error
#         self.slurm_script_path = Path(configs['yggdrasil_script_dir']) / "slurm_manager.sh"  # Adjust this path as necessary

#     async def submit_job(self, script_path):
#         command = [self.slurm_script_path, "submit", script_path]

#         print(">>>> COMMAND: ", command)
#         try:
#             process = await asyncio.create_subprocess_exec(
#                 *command,
#                 stdout=asyncio.subprocess.PIPE,
#                 stderr=asyncio.subprocess.PIPE
#             )
#             stdout, stderr = await asyncio.wait_for(process.communicate(), self.command_timeout)

#             if process.returncode != 0:
#                 logging.error("Error submitting job. STDOUT: %s, STDERR: %s", stdout.decode(), stderr.decode())
#                 return None

#             logging.debug(f"Slurm RAW submit output: {stdout}")
#             logging.debug(f"STDOUT from slurm_manager.sh: {stdout.decode().strip()}")
#             logging.debug(f"STDERR from slurm_manager.sh: {stderr.decode().strip()}")
#             stdout_decoded = stdout.decode().strip()
#             logging.debug(f"Slurm submit output: {stdout_decoded}")

#             # Improved regex to capture the job ID from a string like "Submitted batch job 123456"
#             match = re.search(r'Submitted batch job (\d+)', stdout_decoded)
#             job_id = match.group(1) if match else None

#             if job_id:
#                 logging.info(f"Job submitted with ID: {job_id}")
#                 return job_id
#             else:
#                 logging.error("Failed to extract job ID from sbatch output.")

#         except asyncio.TimeoutError:
#             logging.error("Timeout while submitting job.")
#         except Exception as e:
#             logging.error(f"Unexpected error: {e}")

#         return None

#     async def monitor_job(self, job_id, sample):
#         """Monitors the specified job and calls the sample's post-process method based on job status."""
#         while True:
#             status = await self._job_status(job_id)
#             print(f">>>> RECEIVED STATUS: {status}")
#             if status in ["COMPLETED", "FAILED", "CANCELLED"]:
#                 logging.info(f"Job {job_id} status: {status}")
#                 self.check_status(job_id, status, sample)
#                 break
#             await asyncio.sleep(self.polling_interval)

#     async def _job_status(self, job_id):
#         command = [self.slurm_script_path, "monitor", job_id]
#         try:
#             process = await asyncio.create_subprocess_exec(
#                 *command,
#                 stdout=asyncio.subprocess.PIPE,
#                 stderr=asyncio.subprocess.PIPE
#             )
#             stdout, stderr = await asyncio.wait_for(process.communicate(), self.command_timeout)

#             if process.returncode == 0:
#                 return stdout.decode().strip()

#         except asyncio.TimeoutError:
#             logging.error(f"Timeout while checking status of job {job_id}.")
#         except Exception as e:
#             logging.error(f"Unexpected error while checking status of job {job_id}: {e}")

#         return None

#     @staticmethod
#     def check_status(job_id, status, sample):
#         """
#         Checks the status of a job and calls the appropriate method on the sample object.

#         Args:
#             job_id (str): The job ID.
#             status (str): The status of the job.
#             sample (object): The sample object (must have a post_process method and id attribute).
#         """
#         print(f"Job {job_id} status: {status}")
#         if status == "COMPLETED":
#             print(f"Sample {sample.id} processing completed.")
#             sample.post_process()
#             sample.status = "completed"
#         elif status in ["FAILED", "CANCELLED"]:
#             sample.status = "failed"
#             print(f"Sample {sample.id} processing failed.")


#################################################################################################
######### CLASS BELOW ASSUMES ACCESS TO THE HOST SYSTEM TO SUBMIT SLURM JOBS ####################
#################################################################################################

class SlurmJobManager:
    """
    Manages the submission and monitoring of Slurm jobs.

    Attributes:
        polling_interval (float): Interval for polling job status.
        command_timeout (float): Timeout for Slurm commands.
    """
    def __init__(self, polling_interval=10.0, command_timeout=8.0):
        """
        Initialize the SlurmJobManager with specified polling interval and command timeout.

        Args:
            polling_interval (float): Interval for polling job status. Defaults to 10.0 seconds.
            command_timeout (float): Timeout for Slurm commands. Defaults to 8.0 seconds.
        """
        self.polling_interval = configs.get("job_monitor_poll_interval", polling_interval)
        self.command_timeout = command_timeout


    async def submit_job(self, script_path):
        """
        Submits a Slurm job using the specified script.

        Args:
            script_path (str): Path to the Slurm script.

        Returns:
            str: The job ID if submission is successful, None otherwise.
        """
        sbatch_command = ["sbatch", script_path]
        try:
            result = await asyncio.create_subprocess_exec(
                *sbatch_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(result.communicate(), self.command_timeout)

            if result.returncode != 0:
                logging.error("Error submitting job. Details: %s", stderr.decode())
                return None

            match = re.search(r'\d+', stdout.decode())
            job_id = match.group() if match else None

            if job_id:
                logging.info(f"Job submitted with ID: {job_id}")
                return job_id
        except asyncio.TimeoutError:
            logging.error("Timeout while submitting job.")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

        return None

    # async def monitor_job(self, job_id, callback=None):
    #     while True:
    #         status = await self._job_status(job_id)
    #         if status in ["COMPLETED", "FAILED", "CANCELLED"]:
    #             logging.info(f"Job {job_id} status: {status}")
    #             if callback:
    #                 callback(job_id, status)
    #             break
    #         await asyncio.sleep(self.polling_interval)

    async def monitor_job(self, job_id, sample):
        """
        Monitors the specified job and delegates status handling to the check_status method.

        This method continuously checks the status of a Slurm job until it completes or fails.
        Depending on the final status, it calls the check_status method to handle the sample accordingly.

        Args:
            job_id (str): The job ID.
            sample (object): The sample object with id attribute.
        """
        logging.debug(f"[{sample.id}] Job {job_id} submitted for monitoring.")
        while True:
            status = await self._job_status(job_id)
            if status in ["COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"]:
                # logging.info(f"Job {job_id} status: {status}")
                self.check_status(job_id, status, sample)
                break
            await asyncio.sleep(self.polling_interval)


    async def _job_status(self, job_id):
        """
        Retrieves the status of a Slurm job.

        Args:
            job_id (str): The job ID.

        Returns:
            str: The status of the job.
        """
        sacct_command = f"sacct -n -X -o State -j {job_id}"
        try:
            process = await asyncio.create_subprocess_shell(
                sacct_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()

            if stderr:
                logging.error(f"sacct stderr: {stderr.decode().strip()}")

            if stdout:
                stdout_decoded = stdout.decode().strip()
                logging.debug(f"sacct stdout for job {job_id}: {stdout_decoded}")
                return stdout_decoded
        except asyncio.TimeoutError:
            logging.error(f"Timeout while checking status of job {job_id}.")
        except Exception as e:
            logging.error(f"Unexpected error while checking status of job {job_id}: {e}")

        return None
    
    @staticmethod
    def check_status(job_id, status, sample):
        """
        Monitors the specified job and delegates status handling to the check_status method.

        This method continuously checks the status of a Slurm job until it completes or fails.
        Depending on the final status, it calls the check_status method to handle the sample accordingly.

        Args:
            job_id (str): The job ID.
            sample (object): The sample object with id attribute.
        """
        logging.debug(f"Job {job_id} status: {status}")
        if status == "COMPLETED":
            logging.info(f"Sample {sample.id} processing completed.")
            sample.post_process()
            sample.status = "completed"
        elif status in ["FAILED", "CANCELLED", "TIMEOUT"]:
            sample.status = "failed"
            logging.info(f"Sample {sample.id} processing failed.")