import logging
from look2bq.jobs import QueryRunner, SlugQueryRunner, Look2Bq, BigqueryExporter
import yaml


class YamlExecuter:
    def __init__(self, yaml_path="") -> None:
        with open(yaml_path, "r+") as yaml_file:
            self.plan = yaml.safe_load(yaml_file.read())
        self._initiate_jobs()
        self.store = {}

    def _initiate_jobs(self):

        self._jobs = {
            "look": QueryRunner(),
            "bigquery": BigqueryExporter(),
            "look_from_id": SlugQueryRunner(),
            "look2bq": Look2Bq(),
        }

    def enable(self, job):
        self._jobs[job.name] = job

    def disable(self, job):
        if job.name in self.job:
            del self._jobs[job.name]

    def enable_all(self):
        raise NotImplementedError

    def disable_all(self):
        self._jobs = {}

    def execute(self):
        for task, details in self.plan.items():
            for job_name, job_plan_detail in details.items():
                logging.warning(str(job_name))
                job = self._jobs.get(job_name)
                is_valid, error = job.is_valid(job_plan_detail)
                if not is_valid:
                    raise AssertionError(f"{job_name} : {error}")

                job.get_dependents(self.store)
                response, data = job.run_from_plan(job_plan_detail)
                if response:
                    self.store[job_name] = data
                else:
                    # TODO this is not ideal error handling
                    raise RuntimeError(
                        f"{job_name} : Incorrect Credentials or Network issue or incorrect argument format"
                    )
        return str(data)
