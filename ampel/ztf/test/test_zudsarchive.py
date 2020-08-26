
import pytest
import pickle
import time
import warnings
from os.path import join, abspath, dirname

from sqlalchemy import select, create_engine, MetaData
import sqlalchemy
from sqlalchemy.sql.functions import count
from sqlalchemy.exc import SAWarning
from numbers import Number

from ampel.ztf.t0.ZUDSArchiveUpdater import ZUDSArchiveUpdater
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
def alert_archive(temp_database, zuds_alert_generator):
    updater = ZUDSArchiveUpdater(temp_database)
    for alert in zuds_alert_generator():
        assert updater.insert_alert(alert)
    yield temp_database, zuds_alert_generator

def split_numeric(d):
    """Split dict into numeric and non-numeric values, for use with pytest.approx"""
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

def assert_alerts_equivalent(a, b):
    assert a.keys() == b.keys()
    for k in a:
        if k in ('light_curve', 'candidate', 'publisher'):
            continue
        if isinstance(a[k], float):
            assert a[k] == pytest.approx(b[k])
        else:
            assert a[k] == b[k]
    na , oa = split_numeric(a['candidate'])
    nb , ob = split_numeric(b['candidate'])
    assert na == pytest.approx(nb)
    assert oa == ob
    assert len(a['light_curve']) == len(b['light_curve'])
    mjd = lambda d: d['mjd']
    for pa, pb in zip(sorted(a['light_curve'], key=mjd), sorted(b['light_curve'], key=mjd)):
        na , oa = split_numeric(pa)
        nb , ob = split_numeric(pb)
        assert na == pytest.approx(nb)
        assert oa == ob

def test_insert_unique_alerts(temp_database, zuds_alert_generator):
    db = ZUDSArchiveUpdater(temp_database)
    connection = db._connection
    meta = db._meta
    candids = set()
    for alert in zuds_alert_generator():
        # alerts are unique
        assert alert['candid'] not in candids
        candids.add(alert['candid'])

        assert db.insert_alert(alert)
    rows = connection.execute(select([meta.tables['candidate'].c.candid])).fetchall()
    db_candids = sorted([tup[0] for tup in rows])
    assert sorted(list(candids)) == db_candids

def test_partitioned_read_single(alert_archive):
    temp_database, alert_generator = alert_archive
    db = ZUDSArchiveDB(temp_database)
    reco_alerts = list(db.get_alerts_in_time_range(0, 1e8, group_name='testy', with_cutouts=True))
    alerts = list(alert_generator())
    assert len(alerts) >= 2
    assert len(reco_alerts) == len(alerts)
    for alert, reco_alert in zip(alerts, reco_alerts):
        assert_alerts_equivalent(alert, reco_alert)

def test_partitioned_read_double(alert_archive):
    import itertools
    temp_database, alert_generator = alert_archive
    db1 = ZUDSArchiveDB(temp_database)
    db2 = ZUDSArchiveDB(temp_database)
    kwargs = dict(group_name='testy', block_size=2)
    
    l1 = list((alert['candid'] for alert in itertools.islice(db1.get_alerts_in_time_range(0, 1e8, **kwargs), 5)))
    l2 = list((alert['candid'] for alert in db2.get_alerts_in_time_range(0, 1e8, **kwargs)))
    total = len(list(alert_generator()))
    assert total >= 5
    assert len(l1) == 5, "first client sees all alerts it consumed"
    assert len(l2) == total-4, "alerts in uncommitted block read by second client"
    assert set(l1).intersection(l2) == {l1[-1]}, "both clients see alert in last partial block, as it was never committed"