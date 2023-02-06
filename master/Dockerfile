FROM python:3.7-slim

WORKDIR /app

RUN groupadd -r webservice && useradd --no-log-init -r -g webservice webservice

RUN pip install requests

COPY . .

USER webservice:webservice

EXPOSE 8080

CMD ["python", "master.py"]