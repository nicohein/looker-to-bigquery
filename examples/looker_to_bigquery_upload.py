from ast import IfExp
from look2bq.jobs import Look2Bq

job = Look2Bq()
job.run(qid="E38SPG0QSsn7DqEav1c2i5",destination="project-name.dataset.tablename",if_exists="replace")