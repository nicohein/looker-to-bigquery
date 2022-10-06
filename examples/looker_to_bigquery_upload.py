from look2bq.jobs import Look2Bq

job = Look2Bq()
job.run(qid="QueryIdCodeCopiedFromUrl", destination="project.dataset.tablename", if_exists="replace")