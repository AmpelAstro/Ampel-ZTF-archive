
import pytest
import pickle
import time
import warnings
from os.path import join, abspath, dirname

from sqlalchemy import select, create_engine, MetaData
import sqlalchemy
from sqlalchemy.sql.functions import count
from sqlalchemy.exc import SAWarning

from ampel.ztf.pipeline.t0.ZUDSArchiveUpdater import ZUDSArchiveUpdater
from ampel.ztf.archive.ZUDSArchiveDB import ZUDSArchiveDB

@pytest.fixture
def temp_database(zudsarchive):
    """
    Yield archive database, dropping all rows when finished
    """
    engine = create_engine(zudsarchive)
    meta = MetaData()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=SAWarning)
        meta.reflect(bind=engine)
    try:
        with engine.connect() as connection:
            for name, table in meta.tables.items():
                if name != 'versions':
                    connection.execute(table.delete())
        yield zudsarchive
    finally:
        with engine.connect() as connection:
            for name, table in meta.tables.items():
                if name != 'versions':
                    connection.execute(table.delete())

@pytest.fixture
def alerts():
    with open(join(abspath(dirname(__file__)), 'test-data', 'zuds_alert.test.pkl'), 'rb') as f:
        return pickle.load(f)

@pytest.fixture
def alert_archive(temp_database, zuds_alert_generator):
    updater = ZUDSArchiveUpdater(temp_database)
    for alert in zuds_alert_generator():
        assert updater.insert_alert(alert, 0)
    yield temp_database, zuds_alert_generator

from numbers import Number

def split_numeric(d):
    num = {}
    not_num = {}
    for k,v in d.items():
        if isinstance(v, Number):
            num[k] = v
        else:
            not_num[k] = v
    return num, not_num

def test_alerts_are_unique(zuds_alert_generator):
    candids = set()
    from collections import defaultdict
    candids = defaultdict(list)
    for alert in zuds_alert_generator():
        candids[alert['candid']].append(alert)
    for k, v in candids.items():
        for a,b in zip(v[:-1],v[1:]):
            na , oa = split_numeric(a['candidate'])
            nb , ob = split_numeric(b['candidate'])
            assert na == pytest.approx(nb)
            assert oa == ob
            assert len(a['light_curve']) == len(b['light_curve'])
            for pa, pb in zip(a['light_curve'], b['light_curve']):
                for k in pa:
                    if k not in pb:
                        continue
                    try:
                        assert pa[k] == pytest.approx(pb[k])
                    except TypeError:
                        assert pa[k] == pb[k]

def test_insert_unique_alerts(temp_database, alerts):
    processor_id = 0
    db = ZUDSArchiveUpdater(temp_database)
    connection = db._connection
    meta = db._meta
    timestamps = []
    candids = set()
    for alert in alerts:
        # alerts are unique
        assert alert['candid'] not in candids
        candids.add(alert['candid'])
        
        # # (candid,pid) is unique within an alert packet
        # prevs = dict()
        # for idx, candidate in enumerate([alert['candidate']] + alert['prv_candidates']):
        #     key = (candidate['candid'], candidate['pid'])
        #     assert key not in prevs
        #     prevs[key] = candidate
        del alert['candidate']['mqid']
        timestamps.append(int(time.time()*1e6))
        assert db.insert_alert(alert, timestamps[-1])
    rows = connection.execute(select([meta.tables['candidate'].c.ingestion_time])).fetchall()
    db_timestamps = sorted([tup[0] for tup in rows])
    assert timestamps == db_timestamps
    rows = connection.execute(select([meta.tables['candidate'].c.candid])).fetchall()
    db_candids = sorted([tup[0] for tup in rows])
    assert sorted(list(candids)) == db_candids

def test_partitioned_read_single(alert_archive):
    temp_database, alert_generator = alert_archive
    db = ZUDSArchiveDB(temp_database)
    reco_alerts = list(db.get_alerts_in_time_range(0, 1e8, group_name='testy', with_cutouts=True))
    alerts = list(alert_generator())
    assert len(reco_alerts) == len(alerts)
    for alert, reco_alert in zip(alerts, reco_alerts):
        assert alert.keys() == reco_alert.keys()
        for k in alert:
            if k in ('light_curve', 'candidate', 'publisher'):
                continue
            if isinstance(alert[k], float):
                assert alert[k] == pytest.approx(reco_alert[k])
            else:
                assert alert[k] == reco_alert[k]
        assert sorted(alert['candidate'].keys()) == sorted(reco_alert['candidate'].keys())
        

def test_partitioned_read_double(alert_archive, alerts):
    import itertools
    temp_database, alert_generator = alert_archive
    db1 = ZUDSArchiveDB(temp_database)
    db2 = ZUDSArchiveDB(temp_database)
    kwargs = dict(group_name='testy', block_size=2)
    
    l1 = list((alert['candid'] for alert in itertools.islice(db1.get_alerts_in_time_range(0, 1e8, **kwargs), 5)))
    l2 = list((alert['candid'] for alert in db2.get_alerts_in_time_range(0, 1e8, **kwargs)))
    total = len(list(alert_generator()))
    assert set(l1).intersection(l2) == {l1[-1]}, "both clients see alert in last partial block, as it was never committed"
    assert len(l1) == 5, "first client sees all alerts it consumed"
    assert len(l2) == total-4, "alerts in uncommitted block read by second client"