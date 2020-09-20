FROM python:3.8.3
ENV ROOT_GTFS_PATH /data/

WORKDIR /app
# Downloading the dataset
RUN mkdir /data \
    && curl -o /data/gtfs.zip https://www.vbb.de/media/download/2029 \
    && unzip /data/gtfs.zip -d /data \
    && rm /data/gtfs.zip

# Installing requirements
COPY requirements.txt /app
RUN pip install -r /app/requirements.txt

COPY vbb_loader /app
CMD ["python", "/app/main.py"]