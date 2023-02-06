FROM python:3.7-slim

WORKDIR /app

RUN groupadd -r webservice && useradd --no-log-init -r -g webservice webservice

COPY . .

USER webservice:webservice

EXPOSE 8082

CMD ["python", "secondary.py"]