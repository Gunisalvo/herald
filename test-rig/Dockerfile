FROM python:3.7

MAINTAINER Rodrigo Gunisalvo Leite "gunisalvo@gmail.com"

RUN apt-get update -y && \
    apt-get install -y python-pip python-dev python

RUN pip install pipenv

WORKDIR /app

COPY . /app

RUN pipenv install

ENTRYPOINT ["./app-manager.sh"]
