FROM python:3.9.6

ENV PYTHONUNBUFFERED True
ENV APP_HOME /app

WORKDIR $APP_HOME

COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

CMD exec gunicorn --bind :$PORT --workers 1 --threads 4 --timeout 0 main:app