import logging
import os
from google.cloud import bigquery
from look2bq.core import Query, Job
import datetime as dt
import pandas_gbq
import looker_sdk
import pandas as pd


class QueryRunner(Job):
    def __init__(self) -> None:
        super().__init__()
        self.name = "look"
        self.required_detail = ["model", "view", "fields", "pivots", "filters"]
        self.sdk = looker_sdk.init40()
        self.result = pd.DataFrame()

    def run(
        self,
        query: Query,
        result_format: str = "csv",
        to_file: bool = False,
        filename: str = "",
    ) -> str:
        self.result = self.sdk.run_inline_query(
            result_format, body=query.to_json(), apply_formatting=True
        )
        if to_file:
            with open(filename, "w+") as file:
                file.write(self.result)
        return self.result

    def run_from_plan(self, plan):
        query = Query.from_dict(plan)
        try:
            json_object = self.run(query, "json")
        except Exception:
            return False, None
        data = pd.read_json(str(json_object))
        data.columns = ["_".join(col.split(".")) for col in data.columns]
        return True, data


class SlugQueryRunner(Job):
    def __init__(self) -> None:
        super().__init__()
        self.name = "look_from_id"
        self.required_detail = ["qid"]
        self.sdk = looker_sdk.init40()

    def run(
        self, qid, result_format: str = "csv", to_file: bool = False, filename: str = ""
    ):
        query = self.sdk.query_for_slug(slug=qid)
        self.result = self.sdk.run_query(
            query_id=query.get("id"), result_format=result_format, apply_formatting=True,limit=-1
        )
        if to_file:
            with open(filename, "w+") as file:
                file.write(self.result)
        return self.result

    def run_from_plan(self, plan):
        qid = plan.get("qid")
        try:
            json_object = self.run(qid, "json")
        except Exception:
            return False, None
        data = pd.read_json(str(json_object))
        data.columns = ["_".join(col.split(".")) for col in data.columns]
        return True, data


class Look2Bq(Job):
    def __init__(self) -> None:
        super().__init__()
        self.name = "look2bq"
        self.required_detail = ["qid", "destination", "if_exists"]
        self.slug_q_runner = SlugQueryRunner()
        self.bq_exporter = BigqueryExporter()

    def run(self, qid, destination, if_exists):
        json_object = self.slug_q_runner.run(qid=qid, result_format="json")
        df = pd.read_json(str(json_object))
        df.columns = ["_".join(col.split(".")) for col in df.columns]
        self.bq_exporter.run(df, destination, if_exists)
        return df

    def run_from_plan(self, plan):
        data = self.run(
            qid=plan["qid"],
            destination=plan["destination"],
            if_exists=plan["if_exists"],
        )
        return True, data


class BigqueryExporter(Job):
    def __init__(self) -> None:
        super().__init__()
        self.name = "bigquery"
        self.depends_on = ["look"]
        self.required_detail = ["destination", "if_exists"]

    def run_from_plan(self, plan):
        assert type(self._dependent_data["look"]) == pd.DataFrame
        df = self._dependent_data["look"]
        data = self.run(
            destination=plan["destination"], if_exists=plan["if_exists"], df=df,date=plan.get('date',None)
        )
        return True, data
    

    def _get_impersonated_credentials(self, project_id=None, target_scopes=None, lifetime=500):
        import google.auth
        from google.auth import impersonated_credentials
        
        service_account = os.environ.get("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT")
        if service_account is None:
            return None
        if not target_scopes:
            # change scopes as required https://developers.google.com/identity/protocols/oauth2/scopes
            target_scopes = [
                'https://www.googleapis.com/auth/cloud-platform']
        target_credentials = impersonated_credentials.Credentials(
            source_credentials=google.auth.default(quota_project_id=project_id)[0],  # getting the source credentials using default scope
            target_principal=service_account, 
            target_scopes=target_scopes,  
            lifetime=lifetime)
        return target_credentials
    
    def delete_if_available(self, destination_table, project_id, jobs_project_id, date):
        strdate = date
        cred = self._get_impersonated_credentials()
        logging.warning("Delete existing data if exists.")
        bqclient = bigquery.Client(credentials=cred, project=jobs_project_id)
        sql = f"""
        DELETE FROM {project_id}.{destination_table}
        WHERE CAST(load_time as DATE) = '{strdate}'
        """
        query = bqclient.query(sql)
        query.result()

    def run(self, df, destination, if_exists="replace", date=None):
        parts = destination.split(".")
        project_id = parts[0]
        destination_table = ".".join(parts[1:])
        pandas_gbq.context.credentials = self._get_impersonated_credentials()  # null if impersonation not set
        jobs_project_id = os.environ.get("GOOGLE_PROJECT", "bmg-bigquery-prod")
        pandas_gbq.context.project = jobs_project_id
        if df.shape[0] > 0:
            if if_exists == 'append' and date:
                self.delete_if_available(destination_table, project_id, jobs_project_id, date=date)
            # adding a load timestamp 
            df["load_time"] = dt.datetime.now()
            pandas_gbq.to_gbq(
                df,
                destination_table=destination_table,
                project_id=project_id,
                progress_bar=False,
                if_exists=if_exists,
            )
            return "Successfully uploaded data."
        logging.warning("DataFrame empty. No data loaded.")
        return "DataFrame empty. No data loaded."
 