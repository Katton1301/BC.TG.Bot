FROM python:latest
RUN pip install aiogram
RUN pip install aiokafka
WORKDIR /app
COPY ./app .
ENTRYPOINT ["python", "run.py"]
