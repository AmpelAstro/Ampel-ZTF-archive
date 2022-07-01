

<img align="left" src="https://desycloud.desy.de/index.php/s/6gJs9bYBG3tWFDz/preview" width="150" height="150"/>  
<br>

# ZTF alert archive for AMPEL

This package provides an API to store the elements of ZTF alerts in a postgres database, de-duplicating repeated elements of the alert history. It also supports history queries by object id, sky coordinates, and time.

# Local setup (at least v3 branch is required)

You need a running docker daemon. Ensure that it works. On MacOS, you can install docker (with its daemon) by running `brew install --cask docker`. After that, you will need to launch the Docker desktop application and give it the permissions it asks for. 

You will also need `poetry` to proceed.

After ensuring both, clone the repo, `cd` into it and run `poetry install -E server`, followed by `poetry run pytest --cov=ampel`. If no errors occur, you are good to go.

NOTE OF CAUTION: The docker instance is not persistent. So it will be prudent to run the next steps (launching the docker instance and launching the web server) in two `tmux` or `screen` sessions.

To start the test server, simply run the bash script: 
`chmod +x start_test_server.sh; ./start_test_server.sh`

This will prompt you with an environment variable (NOTE: your port will be different): 
`ARCHIVE_URI="postgresql://ampel:seekrit@localhost:55039/ztfarchive")`

Open a new terminal instance and set this environment variable: `export ARCHIVE_URI="postgresql://ampel:seekrit@localhost:55039/ztfarchive")`

You will also need to set another one: 
`export ROOT_PATH=/api/ztf/archive/v3`

Now you can launch the web server with 
`uvicorn ampel.ztf.archive.server.app:app`

This listens on `localhost:8000`.

Cool, now you have an empty database, and a webserver that does not give you access. We want to change that. First, you will need to manually insert a token into the postgres database. To do this, check under which docker id the postgres-service runs:
`docker ps`

Check for the CONTAINER ID of the postgres. It will look like `5f1d16589f91`. Now execute `docker exec -it 5f1d16589f91 bash` (replace the ID after `-it` with the ID of your postgres docker instance). This will let you enter a bash shell inside the docker VM. Now run
`psql -U ampel -d ztfarchive` and then
`INSERT INTO access_token (owner) VALUES ('mememe');`

To retrieve your token, run
`SELECT * FROM access_token;`
and then copy the API token from there (it will look like `3d752b61-4288-4a92-9aac-6124ce0a207b`).

Tadaa, now you need to fill the database with alerts.
