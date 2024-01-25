
FROM python:3.9

COPY . /opt/app
WORKDIR /opt/app

RUN apt-get update && apt-get install -y python3-pip


RUN pip install -r requirements.txt
