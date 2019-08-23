FROM python:2-slim-stretch

WORKDIR /usr/local/src

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /usr/local/monetax-cron

ENV APP_HOME /usr/local/monetax-cron
ENV PS1 "\[\033[01;32m\]@\h\[\033[00m\]: $ [\W] "

CMD ["python", "./monetax_cron.py" ,"autodebet", "koordinatwp", "offline", "penarikan", "pgomset"]
