

<img align="left" src="https://desycloud.desy.de/index.php/s/6gJs9bYBG3tWFDz/preview" width="150" height="150"/>  
<br>

# ZTF alert archive for AMPEL

This package provides an API to store the elements of ZTF alerts in a postgres database, de-duplicating repeated elements of the alert history. It also supports history queries by object id, sky coordinates, and time.

# Local setup (at least v3 branch is required)

You need a running docker daemon. Ensure that it works. On MacOS, you can install docker (with its daemon) by running `brew install --cask docker`. After that, you will need to launch the Docker desktop application and give it the permissions it asks for. 

You will also need `poetry` to proceed.

After ensuring both, clone the repo, `cd` into it and run `poetry install -E server`, followed by `poetry run pytest --cov=ampel`. If no errors occur, you are good to go.

To start the test server, simply run the bash script: 
`chmod +x start_test_server.sh; ./start_test_server.sh`

This will prompt you with an environment variable (NOTE: your port will be different): 
`ARCHIVE_URI="postgresql://ampel:seekrit@localhost:55039/ztfarchive")`

Open a new terminal and export this environment variable: `export ARCHIVE_URI="postgresql://ampel:seekrit@localhost:55039/ztfarchive")`

You will also need to export another one: 
`export ROOT_PATH=/api/ztf/archive/v3`

Now you can launch the web server with 
`uvicorn ampel.ztf.archive.server.app:app`

This listens on `localhost:8000`