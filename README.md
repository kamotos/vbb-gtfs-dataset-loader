# VBB GTFS dataset loader
This application loads the VBB GTFS dataset into crate.io

# Requirements

Make sure you have:

- [docker](https://docs.docker.com/get-docker/) installed
- [docker-compose](https://docs.docker.com/compose/install/) installed

# How to run?

- `docker-compose up -d db`
- `docker-compose run --rm --entrypoint bash app`
- `python main.py` 

Depending on your machine it will take some time (~16 minutes on my i5-3340M CPU @ 2.70GHz, 8GB RAM rusty laptop) to load the different files in `vbb_loader.models.tables` 
  
