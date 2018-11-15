
from os.path import abspath, join, dirname
import pytest

from ampel.test.fixtures import docker_service

@pytest.fixture(scope="session")
def kafka():
	gen = docker_service('spotify/kafka', 9092,
	    environ={'ADVERTISED_HOST': '127.0.0.1'},
	    port_mapping={9092:9092},
	    healthcheck='$KAFKA_HOME/bin/kafka-topics.sh --zookeeper localhost:2181 --list')
	port = next(gen)
	yield '127.0.0.1:{}'.format(port)

@pytest.fixture(scope="session")
def kafka_stream(kafka, alert_tarball):
	import itertools
	from confluent_kafka import Producer
	from ampel.pipeline.t0.load.TarballWalker import TarballWalker
	atat = TarballWalker(alert_tarball)
	producer = Producer({'bootstrap.servers': kafka})
	for i,fileobj in enumerate(itertools.islice(atat.get_files(), 0, 1000, 1)):
		producer.produce('ztf_20180819_programid1', fileobj.read())
	producer.flush()
	yield kafka

@pytest.fixture(scope='session')
def alert_tarball():
	return join(dirname(__file__), '..', 'alerts', 'ztf_public_20180819_mod1000.tar.gz')

@pytest.fixture(scope='session')
def alert_generator(alert_tarball):
	import itertools
	import fastavro
	from ampel.pipeline.t0.load.TarballWalker import TarballWalker
	def alerts(with_schema=False):
		atat = TarballWalker(alert_tarball)
		for fileobj in itertools.islice(atat.get_files(), 0, 1000, 1):
			reader = fastavro.reader(fileobj)
			alert = next(reader)
			if with_schema:
				yield alert, reader.schema
			else:
				yield alert
	return alerts

@pytest.fixture(scope='session')
def lightcurve_generator(alert_generator):
	from ampel.utils.ZIAlertUtils import ZIAlertUtils
	def lightcurves():
		for alert in alert_generator():
			lightcurve = ZIAlertUtils.to_lightcurve(content=alert)
			assert isinstance(lightcurve.get_photopoints(), tuple)
			yield lightcurve

	return lightcurves

@pytest.fixture(scope='session')
def transientview_generator(alert_generator):
	from ampel.utils.ZIAlertUtils import ZIAlertUtils
	from ampel.base.ScienceRecord import ScienceRecord
	from datetime import datetime
	from numpy import random
	def views():
		for alert in alert_generator():
			results = [
				{
					'versions': {'py': 1.0, 'run_config': 1.0},
					'dt': datetime.utcnow().timestamp(),
					'duration': 0.001,
					'results': {'foo': random.uniform(0,1), 'bar': random.uniform(0,1)}
				}
				for _ in range(random.poisson(1))
			]
			for r in results:
				if random.binomial(1, 0.5):
					del r['results']
					r['error'] = 512
			records = [ScienceRecord(alert['objectId'], 'FancyPants', None, results)]
			tw = ZIAlertUtils.to_transientview(content=alert, science_records=records)
			yield tw

	return views

@pytest.fixture
def ampel_alerts(alert_generator):
	from ampel.pipeline.t0.alerts.AlertSupplier import AlertSupplier
	from ampel.ztf.pipeline.t0.alerts.ZIAlertShaper import ZIAlertShaper
	from ampel.base.AmpelAlert import AmpelAlert
	def ampelize(shaped_alert):
		return AmpelAlert(shaped_alert['tran_id'], shaped_alert['ro_pps'], shaped_alert['ro_uls'])
	yield map(ampelize, AlertSupplier(alert_generator(), ZIAlertShaper()))

@pytest.fixture
def latest_schema():
	with open(join(dirname(__file__), '..', '..', '..', 'alerts', 'schema_2.0.json')) as f:
		return json.load(f)
