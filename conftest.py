
from os.path import abspath, join, dirname
from os import environ
import pytest

collect_ignore = ['ampel/ztf/test/fixtures.py']
pytest_plugins = ['ampel.ztf.test.fixtures']


@pytest.fixture(scope='session')
def transientview_generator(alert_generator):
	from ampel.util.ZIAlertUtils import ZIAlertUtils
	from ampel.content.T2Record import T2Record
	from datetime import datetime
	from numpy import random
	def views():
		for alert in alert_generator():
			results = [
				{
					'versions': {'py': 1.0, 'run_config': 1.0},
					'dt': datetime.utcnow().timestamp(),
					'duration': 0.001,
					'body': {'foo': random.uniform(0,1), 'bar': random.uniform(0,1)}
				}
				for _ in range(random.poisson(1))
			]
			for r in results:
				if random.binomial(1, 0.5):
					del r['body']
					r['error'] = 512
			records = [T2Record(alert['objectId'], 'FancyPants', None, results)]
			tw = ZIAlertUtils.to_transientview(content=alert, science_records=records)
			yield tw

	return views

@pytest.fixture
def alerts(alert_generator):
	from ampel.t0.alerts.AlertSupplier import AlertSupplier
	from ampel.ztf.t0.alerts.ZIAlertShaper import ZIAlertShaper
	from ampel.alert.PhotoAlert import PhotoAlert
	def ampelize(shaped_alert):
		return PhotoAlert(shaped_alert['tran_id'], shaped_alert['ro_pps'], shaped_alert['ro_uls'])
	yield map(ampelize, AlertSupplier(alert_generator(), ZIAlertShaper()))

@pytest.fixture
def latest_schema():
	with open(join(dirname(__file__), '..', '..', '..', 'alerts', 'schema_2.0.json')) as f:
		return json.load(f)
