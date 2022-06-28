

<img align="left" src="https://desycloud.desy.de/index.php/s/6gJs9bYBG3tWFDz/preview" width="150" height="150"/>  
<br>

# ZTF alert archive for AMPEL

This package provides an API to store the elements of ZTF alerts in a postgres database, de-duplicating repeated elements of the alert history. It also supports history queries by object id, sky coordinates, and time.

# Local setup

You need a running docker daemon. Ensure that it works. On MacOS, you can install docker (with its daemon) by running `brew install --cask docker`. After that, you will need to launch the Docker desktop application and give it the permissions it asks for. 

You will also need `poetry` to proceed.


After ensuring both, clone the repo, `cd` into it and run `poetry install -E server`, followed by `poetry run pytest --cov=ampel`. If no errors occur, you are good to go.