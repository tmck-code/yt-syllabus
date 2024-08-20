FROM python:3.12-slim

RUN apt update && apt install -y \
  default-jre \
  procps

ADD requirements.txt .
RUN python3 -m pip install -r requirements.txt

WORKDIR /home
ADD . .
