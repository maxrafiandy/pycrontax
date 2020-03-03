FROM python:2-stretch

WORKDIR /usr/local/src

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /usr/local/monetax-cron

ENV APP_HOME /usr/local/monetax-cron

CMD ["python", "./monetax_cron.py" ,"autodebet", "koordinatwp", "offline", "penarikan", "pgomset"]