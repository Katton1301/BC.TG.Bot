FROM python:latest
COPY requirements.txt .
RUN pip install -r requirements.txt
WORKDIR /app
COPY ./app .
ENTRYPOINT ["python", "run.py"]
