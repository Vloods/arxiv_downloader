# Arxiv Downloader
Script to download articles from Arxiv. Loads the metadata, PDF and 
txt files for each article.

## Getting Started
These instructions will get you a copy of the project up and running 
on your local machine for development and testing purposes.

### Prerequsites
Docker and Docker-compose must be installed for successful launch.

### Installing
1. Download the repository;

2. You need to add the *config.ini* file to the configs folder. **config.ini**  example:

	    [DEFAULT]
	    ACCESS_KEY = <key>
	    SECRET_KEY = <key>


4. Open a terminal and go to the repository directory.

		cd <path to repos>


5. Build

		docker-compose build


6. Run

		docker-compose up
