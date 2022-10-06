# Looker to BigQuery (look2bq)

## Purpose 🔮

The code in this repository allows to export data from Looker to BigQuery.
This task might sound redundant because data presented in Looker are supposed to be in a database like BigQuery in the first place. However, there is one key exception: Lookers `system_activity` explores.

There are multiple reasons why it is worth exporting at least parts of this data:

1. history: the data in `system_activity` is live, or may be stored between 30 and 90 days.
1. performance: the Looker database is a transactional system not optimized for analytics - on a busy instance running usage reports is not the most efficient
1. security: storing the `system_activity` data for longer than 90 days might be required for security reasons
1. flexibility: loading data like `users` and `user_groups` periodically allows time series analysis on entities that are otherwise static.

This package provides all code required to load generic data from Looker to BigQuery.
It also entails the definitions of some example queries.

## Installation 📦

- Run `pip install -r requirements.txt`
- Set the following Environment variables

```bash
export LOOKER_BASE_URL=<looker_url>
export LOOKER_CLIENT_ID=<client_id>
export LOOKER_CLIENT_SECRET=<secret>
export GOOGLE_PROJECT=project-name
export GOOGLE_IMPERSONATE_SERVICE_ACCOUNT=service-acc-name@project-name.iam.gserviceaccount.com
```

- Modify the config.yaml file according to requirements
- Enable the required jobs (only Looker job is enabled by default) and run in `main.py` file
- It's suggested to use prefix 'look2q' for tables that are exported using this module to traceback reponsiblity and error details in future.

## Usage 💻

Please go though the `look2bq/tests` folder to know about the arguments and it's type that can be passed for a job

To run the tests use:

```bash
cd tests  
pytest -vv test_bigquery.py
pytest -vv test_look_from_id.py
pytest -vv test_look2bq.py
pytest -vv test_looker.py
```

To check with your own user:

```
unset GOOGLE_IMPERSONATE_SERVICE_ACCOUNT
```

## Docker Testing 🐋

```bash
docker build -f Dockerfile -t looker-to-bigquery .
```

```
docker run -v ~/.config/gcloud:/root/.config/gcloud  \
    -e LOOKERSDK_BASE_URL=<looker_url> \
    -e LOOKERSDK_CLIENT_SECRET=<secret> \
    -e LOOKERSDK_CLIENT_ID=<client_id> \
    -e GOOGLE_PROJECT=project-name
    -e GOOGLE_IMPERSONATE_SERVICE_ACCOUNT=looker-sys-activity-export-srv@project-name.iam.gserviceaccount.com
    -p 8080:8080 \
    looker-to-bigquery
```

Run URLs:

- [health](http://0.0.0.0:8080/)
- [getdates](http://0.0.0.0:8080/getdates?start_date=2022-01-01&end_date=2022-03-01) 
- [getdates](http://0.0.0.0:8080/getdates?start_date=&end_date=) 
- [execute_config](http://0.0.0.0:8080/execute_config?query_config=events_config&transaction_date=2022-07-19)- 3 times for past, current and future dates
- [execute_query](http://0.0.0.0:8080/execute_query?qid=E38SPG0QSsn7DqEav1c2i5&destination=project-name.looker_system_activity.groups&if_exists=replace)


## Deployment ⚓

```bash
docker build -f Dockerfile -t looker-to-bigquery .
docker tag looker-to-bigquery eu.gcr.io/project-name/looker-to-bigquery:0.0.6
docker tag looker-to-bigquery eu.gcr.io/project-name/looker-to-bigquery:latest
gcloud config set project project-name
gcloud auth configure-docker
docker push eu.gcr.io/project-name/looker-to-bigquery:0.0.6
docker push eu.gcr.io/project-name/looker-to-bigquery:latest
```

### Test the deployment 🧪

Use these json configs:

For yesterdays data:

```json
{}
```

For a given time range (exclusive of end date):

```json
{"start_date": "2022-05-01", "end_date": "2022-08-11"}
```

## Concepts 🛠️

If you are interested in getting to know the concept design of look2bq read here

- `Executer`
  - It is used to execute operations using details from input file
  - Currently look2bq only has YamlExecutor
- `Task`
  - It contains set of jobs sequentially when executed
  - Yaml file contains set of tasks
  - Example is `bqtesttask` in tests/test_bigquery.yaml

- `Job`
  - It is basic block which is responsible for running certain operation
  - keywords under the `taskname` in yaml file are job names that needs to be executed
  - Examples are `look` and `bigquery` inside `bqtesttask` task in `tests/test_bigquery.yaml`
  - Custom jobs could be created using by inheriting and overriding Job abstract object in `look2bq/core.py`

## How to Contribute 🎁

- Added tests in `look2bq/tests` to validate the new feature you add
- Add abstract objects to `look2bq/core.py`
- Add Job objects to `look2bq/jobs.py`
- Add Executer objects to `look2bq/executors.py`
