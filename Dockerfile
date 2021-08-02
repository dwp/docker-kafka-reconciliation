FROM python:3-buster

ENV RECONCILIATION_HOME=/kafka_reconciliation
RUN mkdir RECONCILIATION_HOME
WORKDIR RECONCILIATION_HOME

COPY kafka_reconciliation/main.py ./
COPY kafka_reconciliation/utility ./utility
COPY kafka_reconciliation/requirements.txt ./
RUN pip install -r ./requirements.txt --trusted-host pypi.org --trusted-host files.pythonhosted.org --user
RUN chmod +x ./main.py
ENTRYPOINT ["python", "main.py"]
