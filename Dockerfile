FROM python:3.8.3

COPY gtfs /data
COPY requirements.txt /app
RUN pip install requirements.txt

COPY vbb_loader /app
RUN ["python", "/app/main.py"]