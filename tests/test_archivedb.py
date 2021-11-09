import pytest
import os
import secrets
import time
import operator
from math import isnan
from collections import defaultdict
from sqlalchemy.sql.elements import BindParameter

from sqlalchemy.sql.sqltypes import Binary

from ampel.ztf.archive.ArchiveDB import ArchiveDB
from ampel.ztf.t0.ArchiveUpdater import ArchiveUpdater

from sqlalchemy import select, create_engine, MetaData
import sqlalchemy
from sqlalchemy.sql.functions import count
from sqlalchemy.exc import SAWarning
from sqlalchemy.sql.expression import BooleanClauseList, BinaryExpression, Function
import warnings

from collections.abc import Iterable
import json


def test_walk_tarball(alert_generator):
    alerts = list(alert_generator())
    assert len(alerts) == 30
    assert alerts[0]["publisher"] == "ZTF (www.ztf.caltech.edu)"


def test_insert_unique_alerts(empty_archive, alert_generator):
    processor_id = 0
    db = ArchiveUpdater(empty_archive)
    connection = db._engine.connect()
    meta = db._meta
    timestamps = []
    candids = set()
    for alert, schema in alert_generator(with_schema=True):
        # alerts are unique
        assert alert["candid"] not in candids
        candids.add(alert["candid"])

        # (candid,pid) is unique within an alert packet
        prevs = dict()
        for idx, candidate in enumerate([alert["candidate"]] + alert["prv_candidates"]):
            key = (candidate["candid"], candidate["pid"])
            assert key not in prevs
            prevs[key] = candidate

        timestamps.append(int(time.time() * 1e6))
        db.insert_alert(alert, schema, processor_id, timestamps[-1])
    rows = connection.execute(
        select([meta.tables["alert"].c.ingestion_time])
    ).fetchall()
    db_timestamps = sorted([tup[0] for tup in rows])
    assert timestamps == db_timestamps


def count_previous_candidates(alert):
    upper_limits = sum((1 for c in alert["prv_candidates"] if c["candid"] is None))
    return len(alert["prv_candidates"]) - upper_limits, upper_limits


def test_insert_duplicate_alerts(empty_archive, alert_generator):
    import itertools

    processor_id = 0
    db = ArchiveUpdater(empty_archive)
    connection = db._engine.connect()
    meta = db._meta

    alert, schema = next(alert_generator(with_schema=True))
    detections, upper_limits = count_previous_candidates(alert)

    db.insert_alert(alert, schema, processor_id, int(time.time() * 1e6))
    assert (
        connection.execute(count(meta.tables["alert"].columns.candid)).first()[0] == 1
    )
    assert (
        connection.execute(count(meta.tables["candidate"].columns.candid)).first()[0]
        == 1
    )
    assert (
        connection.execute(count(meta.tables["prv_candidate"].columns.candid)).first()[
            0
        ]
        == detections
    )
    assert (
        connection.execute(
            count(meta.tables["upper_limit"].columns.upper_limit_id)
        ).first()[0]
        == upper_limits
    )

    # inserting the same alert a second time does nothing
    db.insert_alert(alert, schema, processor_id, int(time.time() * 1e6))
    assert (
        connection.execute(count(meta.tables["alert"].columns.candid)).first()[0] == 1
    )
    assert (
        connection.execute(count(meta.tables["candidate"].columns.candid)).first()[0]
        == 1
    )
    assert (
        connection.execute(count(meta.tables["prv_candidate"].columns.candid)).first()[
            0
        ]
        == detections
    )
    assert (
        connection.execute(
            count(meta.tables["upper_limit"].columns.upper_limit_id)
        ).first()[0]
        == upper_limits
    )


def test_insert_duplicate_photopoints(empty_archive, alert_generator):
    processor_id = 0
    db = ArchiveUpdater(empty_archive)
    connection = db._engine.connect()
    meta = db._meta
    from sqlalchemy.sql.expression import tuple_, func
    from sqlalchemy.sql.functions import sum

    # find an alert with at least 1 previous detection
    for alert, schema in alert_generator(with_schema=True):
        detections, upper_limits = count_previous_candidates(alert)
        if detections > 0 and upper_limits > 0:
            break
    assert detections > 0

    db.insert_alert(alert, schema, processor_id, int(time.time() * 1e6))
    assert (
        connection.execute(count(meta.tables["alert"].columns.candid)).first()[0] == 1
    )
    assert (
        connection.execute(count(meta.tables["candidate"].columns.candid)).first()[0]
        == 1
    )
    assert (
        connection.execute(count(meta.tables["prv_candidate"].columns.candid)).first()[
            0
        ]
        == detections
    )
    assert (
        connection.execute(
            count(meta.tables["upper_limit"].columns.upper_limit_id)
        ).first()[0]
        == upper_limits
    )
    assert (
        connection.execute(
            count(meta.tables["alert_prv_candidate_pivot"].columns.alert_id)
        ).first()[0]
        == 1
    )
    assert (
        connection.execute(
            sum(
                func.array_length(
                    meta.tables["alert_prv_candidate_pivot"].columns.prv_candidate_id, 1
                )
            )
        ).first()[0]
        == detections
    )
    assert (
        connection.execute(
            count(meta.tables["alert_upper_limit_pivot"].columns.upper_limit_id)
        ).first()[0]
        == 1
    )
    assert (
        connection.execute(
            sum(
                func.array_length(
                    meta.tables["alert_upper_limit_pivot"].columns.upper_limit_id, 1
                )
            )
        ).first()[0]
        == upper_limits
    )

    # insert a new alert, containing the same photopoints. only the alert and pivot tables should gain entries
    alert["candid"] += 1
    alert["candidate"]["candid"] = alert["candid"]
    db.insert_alert(alert, schema, processor_id, int(time.time() * 1e6))
    assert (
        connection.execute(count(meta.tables["alert"].columns.candid)).first()[0] == 2
    )
    assert (
        connection.execute(count(meta.tables["candidate"].columns.candid)).first()[0]
        == 2
    )
    assert (
        connection.execute(count(meta.tables["prv_candidate"].columns.candid)).first()[
            0
        ]
        == detections
    )
    assert (
        connection.execute(
            count(meta.tables["upper_limit"].columns.upper_limit_id)
        ).first()[0]
        == upper_limits
    )
    assert (
        connection.execute(
            count(meta.tables["alert_prv_candidate_pivot"].columns.alert_id)
        ).first()[0]
        == 2
    )
    assert (
        connection.execute(
            sum(
                func.array_length(
                    meta.tables["alert_prv_candidate_pivot"].columns.prv_candidate_id, 1
                )
            )
        ).first()[0]
        == 2 * detections
    )
    assert (
        connection.execute(
            count(meta.tables["alert_upper_limit_pivot"].columns.upper_limit_id)
        ).first()[0]
        == 2
    )
    assert (
        connection.execute(
            sum(
                func.array_length(
                    meta.tables["alert_upper_limit_pivot"].columns.upper_limit_id, 1
                )
            )
        ).first()[0]
        == 2 * upper_limits
    )


def test_delete_alert(empty_archive, alert_generator):
    processor_id = 0
    db = ArchiveUpdater(empty_archive)
    connection = db._engine.connect()
    meta = db._meta
    from sqlalchemy.sql.expression import tuple_, func
    from sqlalchemy.sql.functions import sum

    alert, schema = next(alert_generator(with_schema=True))
    detections, upper_limits = count_previous_candidates(alert)

    db.insert_alert(alert, schema, processor_id, int(time.time() * 1e6))

    Alert = meta.tables["alert"]
    connection.execute(Alert.delete().where(Alert.c.candid == alert["candid"]))
    assert (
        connection.execute(count(meta.tables["alert"].columns.candid)).first()[0] == 0
    )
    assert (
        connection.execute(count(meta.tables["candidate"].columns.candid)).first()[0]
        == 0
    )
    assert (
        connection.execute(
            count(meta.tables["alert_prv_candidate_pivot"].columns.alert_id)
        ).first()[0]
        == 0
    )
    assert (
        connection.execute(
            sum(
                func.array_length(
                    meta.tables["alert_prv_candidate_pivot"].columns.prv_candidate_id, 1
                )
            )
        ).first()[0]
        == None
    )
    assert (
        connection.execute(
            count(meta.tables["alert_upper_limit_pivot"].columns.upper_limit_id)
        ).first()[0]
        == 0
    )
    assert (
        connection.execute(
            sum(
                func.array_length(
                    meta.tables["alert_upper_limit_pivot"].columns.upper_limit_id, 1
                )
            )
        ).first()[0]
        == None
    )
    # array-joined tables don't participate in delete cascade, because ELEMENT REFERENCES is still not a thing
    # http://blog.2ndquadrant.com/postgresql-9-3-development-array-element-foreign-keys/
    assert (
        connection.execute(count(meta.tables["prv_candidate"].columns.candid)).first()[
            0
        ]
        == detections
    )
    assert (
        connection.execute(
            count(meta.tables["upper_limit"].columns.upper_limit_id)
        ).first()[0]
        == upper_limits
    )


def assert_alerts_equivalent(alert, reco_alert):

    # some necessary normalization on the alert
    fluff = ["pdiffimfilename", "programpi"]
    alert = dict(alert)

    def strip(in_dict):
        out_dict = dict(in_dict)
        for k, v in in_dict.items():
            if isinstance(v, float) and isnan(v):
                out_dict[k] = None
            if k in fluff:
                del out_dict[k]
        return out_dict

    alert["candidate"] = strip(alert["candidate"])
    assert alert.keys() == reco_alert.keys()
    for k in alert:
        if "candidate" in k or k == "publisher":
            pass
        elif k.startswith("cutout"):
            assert alert[k]["stampData"] == reco_alert[k]["stampData"]
        elif isinstance(alert[k], float):
            assert alert[k] == pytest.approx(reco_alert[k])
        else:
            assert alert[k] == reco_alert[k]
    assert len(alert["prv_candidates"]) == len(reco_alert["prv_candidates"])
    prvs = sorted(
        alert["prv_candidates"],
        key=lambda f: (f["jd"], f["candid"] is None, f["candid"]),
    )
    reco_prvs = reco_alert["prv_candidates"]
    try:
        assert [c.get("candid") for c in prvs] == [c.get("candid") for c in reco_prvs]
    except:
        jd_off = lambda cands: [c["jd"] - cands[0]["jd"] for c in cands]
        print(jd_off(prvs))
        print(jd_off(reco_alert["prv_candidates"]))
        raise
    for prv, reco_prv in zip(prvs, reco_prvs):
        prv = strip(prv)
        # remove keys not in original alert (because it came from an older schema)
        for k in set(reco_prv.keys()).difference(prv.keys()):
            del reco_prv[k]
        assert {k for k, v in prv.items() if v is not None} == {
            k for k, v in reco_prv.items() if v is not None
        }
        for k in prv.keys():
            # print(k, prv[k], reco_prv[k])
            try:
                if prv[k] is None:
                    assert reco_prv.get(k) is None
                elif isinstance(prv[k], float):
                    assert prv[k] == pytest.approx(reco_prv[k])
                else:
                    assert prv[k] == reco_prv[k]
            except:
                print(k, prv[k], reco_prv[k])
                raise
    keys = {k for k, v in alert["candidate"].items() if v is not None}
    candidate = {k: v for k, v in alert["candidate"].items() if k in keys}
    reco_candidate = {k: v for k, v in reco_alert["candidate"].items() if k in keys}
    for k in set(alert["candidate"].keys()).difference(keys):
        assert reco_alert["candidate"][k] is None
    for k in reco_candidate:
        if isinstance(reco_candidate[k], float):
            assert reco_candidate[k] == pytest.approx(candidate[k])
        else:
            assert reco_candidate[k] == candidate[k]


def test_get_cutout(empty_archive, alert_generator):
    processor_id = 0
    updater = ArchiveUpdater(empty_archive)
    db = ArchiveDB(empty_archive)
    for idx, (alert, schema) in enumerate(alert_generator(with_schema=True)):
        processor_id = idx % 16
        updater.insert_alert(alert, schema, processor_id, 0)

    for idx, alert in enumerate(alert_generator()):
        processor_id = idx % 16
        cutouts = db.get_cutout(alert["candid"])
        alert_cutouts = {
            k[len("cutout") :].lower(): v["stampData"]
            for k, v in alert.items()
            if k.startswith("cutout")
        }
        assert cutouts == alert_cutouts


def test_serializability(empty_archive, alert_generator):
    """
    Ensure that we can recover avro alerts from the db
    """
    # the python implementation of writer throws more useful errors on schema
    # violations
    from fastavro._write_py import writer
    from fastavro import reader
    from io import BytesIO

    processor_id = 0
    updater = ArchiveUpdater(empty_archive)
    db = ArchiveDB(empty_archive)

    for idx, (alert, schema) in enumerate(alert_generator(with_schema=True)):
        processor_id = idx % 16
        updater.insert_alert(alert, schema, processor_id, 0)

    for idx, alert in enumerate(alert_generator()):
        reco = db.get_alert(alert["candid"], with_history=True, with_cutouts=True)
        # round-trip to avro and back
        f = BytesIO()
        writer(f, schema, [reco])
        deserialized = next(reader(BytesIO(f.getvalue())))


@pytest.fixture(params=["3.2", "3.3"])
def alert_with_schema(request):
    from os.path import join, dirname
    import fastavro

    fname = join(dirname(__file__), "test-data", "schema_{}.avro".format(request.param))
    with open(fname, "rb") as f:
        r = fastavro.reader(f)
        alert, schema = next(r), r.writer_schema
    return alert, schema


def test_schema_update(empty_archive, alert_with_schema):
    from os.path import join, dirname
    from fastavro._write_py import writer
    from fastavro import reader
    from io import BytesIO

    updater = ArchiveUpdater(empty_archive)
    db = ArchiveDB(empty_archive)

    alert, schema = alert_with_schema
    updater.insert_alert(alert, schema, 0, 0)
    reco = db.get_alert(alert["candid"], with_history=True, with_cutouts=True)
    f = BytesIO()
    writer(f, schema, [reco])
    deserialized = next(reader(BytesIO(f.getvalue())))
    assert deserialized.keys() == reco.keys()
    for k in reco:
        if not "candidate" in k or "cutout" in k:
            assert deserialized[k] == reco[k]
    # reconstructed alert from the database may have extra keys
    assert not set(deserialized["candidate"].keys()).difference(
        reco["candidate"].keys()
    )
    old, new = reco["candidate"], deserialized["candidate"]
    for k in new:
        if isinstance(old[k], float):
            assert old[k] == pytest.approx(new[k])
        else:
            assert old[k] == new[k]
    assert len(deserialized["prv_candidates"]) == len(reco["prv_candidates"])
    for old, new in zip(reco["prv_candidates"], deserialized["prv_candidates"]):
        assert not set(old.keys()).difference(new.keys())
        for k in new:
            if new[k] is None:
                # allow missing key to be None
                assert old.get(k) is None
            elif isinstance(old[k], float):
                assert old[k] == pytest.approx(new[k])
            else:
                assert old[k] == new[k]


def test_archive_object(alert_generator, empty_archive):
    updater = ArchiveUpdater(empty_archive)
    from itertools import islice

    for alert, schema in islice(alert_generator(with_schema=True), 10):
        assert schema["version"] == "3.0", "Need alerts with current schema"
        updater.insert_alert(alert, schema, 0, 0)
    # end the transaction to commit changes to the stats tables
    with updater._engine.connect() as connection:
        connection.execute("end")
        connection.execute("vacuum full")
    del updater
    db = ArchiveDB(empty_archive)

    for alert in islice(alert_generator(), 10):
        reco_alert = db.get_alert(alert["candid"], with_history=True, with_cutouts=True)
        # some necessary normalization on the alert
        for k, v in alert["candidate"].items():
            if isinstance(v, float) and isnan(v):
                alert["candidate"][k] = None
        assert_alerts_equivalent(alert, reco_alert)

    alerts = list(islice(alert_generator(), 10))
    candids = [a["candid"] for a in alerts]
    reco_candids = [a["candid"] for a in db.get_alerts(candids)]
    assert reco_candids == candids

    jds = sorted([a["candidate"]["jd"] for a in alerts])
    sec = 1 / (24 * 3600.0)
    reco_jds = [
        a["candidate"]["jd"]
        for a in db.get_alerts_in_time_range(jd_start=min(jds) - sec, jd_end=max(jds) + sec)
    ]
    assert reco_jds == jds

    reco_candids = [
        a["candid"]
        for a in db.get_alerts_in_cone(
            ra=alerts[0]["candidate"]["ra"], dec=alerts[0]["candidate"]["dec"], radius=2.0
        )
    ]
    assert alerts[0]["candid"] in reco_candids

    # for table, stats in db.get_statistics().items():
    #     assert stats['rows'] >= db._connection.execute(db._meta.tables[table].count()).fetchone()[0]


def test_partitioned_read_single(alert_archive):
    db = ArchiveDB(alert_archive)
    alerts = db.get_alerts_in_time_range(jd_start=0, jd_end=1e8, group_name="testy")
    l = list((alert["candid"] for alert in alerts))
    assert len(l) == 10


def test_partitioned_read_double(alert_archive):
    import itertools

    db1 = ArchiveDB(alert_archive)
    db2 = ArchiveDB(alert_archive)
    # kwargs = dict(group_name='testy', block_size=2, with_history=False, with_cutouts=False)
    kwargs = dict(group_name="testy", block_size=2)

    l1 = list(
        (
            alert["candid"]
            for alert in itertools.islice(
                db1.get_alerts_in_time_range(jd_start=0, jd_end=1e8, **kwargs), 5
            )
        )
    )
    l2 = list(
        (alert["candid"] for alert in db2.get_alerts_in_time_range(jd_start=0, jd_end=1e8, **kwargs))
    )

    assert set(l1).intersection(l2) == {
        l1[-1]
    }, "both clients see alert in last partial block, as it was never committed"
    assert len(l1) == 5, "first client sees all alerts it consumed"
    assert len(l2) == 6, "alerts in uncommitted block read by second client"


def test_insert_future_schema(alert_generator, empty_archive):
    db = ArchiveUpdater(empty_archive)

    alert, schema = next(alert_generator(True))
    schema["version"] = str(float(schema["version"]) + 10)
    with pytest.raises(ValueError) as e_info:
        db.insert_alert(alert, schema, 0, 0)


def test_create_topic(alert_archive):
    candids = [595147624915010001, 595193335915010017, 595211874215015018]
    db = ArchiveDB(alert_archive)
    topic = secrets.token_urlsafe()
    topic_id = db.create_topic(
        topic,
        candids,
        "the bird is the word",
    )
    with db._engine.connect() as conn:
        Topic = db._meta.tables["topic"]
        row = conn.execute(Topic.select().where(Topic.c.topic_id==topic_id)).fetchone()
        assert row
        assert len(row["alert_ids"]) == 3

@pytest.mark.parametrize("selection", [slice(None), slice(None,None,1)])
def test_topic_to_read_queue(alert_archive, selection):
    candids = [595147624915010001, 595193335915010017, 595211874215015018]
    db = ArchiveDB(alert_archive)
    topic = secrets.token_urlsafe()
    group = secrets.token_urlsafe()
    db.create_topic(
        topic,
        candids,
        "the bird is the word",
    )
    queue_info = db.create_read_queue_from_topic(topic, group, 2, selection)
    assert queue_info["chunks"] == 2
    assert queue_info["items"] == 3

    assert [alert["candid"] for alert in db.get_chunk_from_queue(group)] == candids[:2]
    assert [alert["candid"] for alert in db.get_chunk_from_queue(group)] == candids[2:]
    assert [alert["candid"] for alert in db.get_chunk_from_queue(group)] == []

def test_cone_search(alert_archive):
    db = ArchiveDB(alert_archive)
    group = secrets.token_urlsafe()
    alerts = list(db.get_alerts_in_cone(ra=0, dec=0, radius=1,  with_history=False, with_cutouts=False, group_name=group))
    assert len(alerts) == 0

@pytest.mark.parametrize("nside", [32, 64, 128])
def test_healpix_search(empty_archive, nside):
    db = ArchiveDB(empty_archive)
    group = secrets.token_urlsafe()
    ipix = 13
    condition, order = db._healpix_search_condition(nside, ipix, -1, 1)
    assert isinstance(condition, BooleanClauseList)
    assert condition.operator == operator.and_

    ops = []
    if nside == 64:
        ops.append(condition.get_children()[0])
    else:
        ops.extend(condition.get_children()[0].get_children())

    nsides = []
    for op in ops:
        assert isinstance(op, BinaryExpression)    
        assert isinstance(func := op.get_children()[0], Function)
        assert func.name == "healpix_ang2ipix_nest"
        assert isinstance(arg := func.clauses.get_children()[0], BindParameter)
        nsides.append(arg.value)
    assert any([n == 64 for n in nsides]), "pixel lookups used indexed expression at least once"
    
