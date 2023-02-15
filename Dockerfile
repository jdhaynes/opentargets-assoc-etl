FROM python:3.9

COPY ./requirements.txt ./requirements.txt
COPY ./etl ./etl
COPY ./tests ./tests

RUN python3 -m pip install -r ./requirements.txt

ENTRYPOINT ["python3", "./etl/run.py"]