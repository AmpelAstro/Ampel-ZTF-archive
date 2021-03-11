
from os.path import abspath, join, dirname
from os import environ
import pytest

pytest_plugins = ['tests.fixtures']

@pytest.fixture
def latest_schema():
	with open(join(dirname(__file__), '..', '..', '..', 'alerts', 'schema_2.0.json')) as f:
		return json.load(f)
