#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/archive/ArchiveDB.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 10.04.2018
# Last Modified Date: 02.12.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Any, Dict, Tuple

from ampel.util.mappings import build_unsafe_dict_id
from ampel.ztf.archive.ArchiveDBClient import ArchiveDBClient
from sqlalchemy import select, and_, bindparam
from sqlalchemy.sql.expression import func
from sqlalchemy.exc import IntegrityError
import sqlalchemy, collections

import logging
log = logging.getLogger('ampel.ztf.archive')

def without_keys(table):
    keys = set(table.primary_key.columns)
    for fk in table.foreign_keys:
        keys.update(fk.constraint.columns)
    return [c for c in table.columns if c not in keys]

class ArchiveDB(ArchiveDBClient):
    """
    """
    _CLIENTS: Dict[bytes, 'ArchiveDB'] = {}
    def __init__(self, *args, **kwargs):
        """
        """
        super().__init__(*args, **kwargs)
        self._alert_id_column = self.get_alert_id_column()

    def _get_alert_column(self, name):
        if 'alert' in self._meta.tables and name in self._meta.tables['alert'].c:
            return getattr(self._meta.tables['alert'].c, name)
        else:
            return getattr(self._meta.tables['candidate'].c, name)

    def get_alert_id_column(self):
        return self._meta.tables['alert'].c.alert_id

    @classmethod
    def instance(cls, *args, **kwargs) -> 'ArchiveDB':
        """
        Get a shared instance of a client with the given connection parameters
        """
        key = build_unsafe_dict_id({"args": args, "kwargs": kwargs})
        if not key in cls._CLIENTS:
            cls._CLIENTS[key] = cls(*args, **kwargs)
        return cls._CLIENTS[key]

    def get_statistics(self):
        """
        """
        stats = {}
        with self._connection.begin() as transaction:
            try:
                sql = "select relname, n_live_tup from pg_catalog.pg_stat_user_tables"
                rows = dict(self._connection.execute(sql).fetchall())
                sql = """SELECT TABLE_NAME, index_bytes, toast_bytes, table_bytes
                         FROM (
                         SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
                             SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
                                     , c.reltuples AS row_estimate
                                     , pg_total_relation_size(c.oid) AS total_bytes
                                     , pg_indexes_size(c.oid) AS index_bytes
                                     , pg_total_relation_size(reltoastrelid) AS toast_bytes
                                 FROM pg_class c
                                 LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                                 WHERE relkind = 'r' AND nspname = 'public'
                         ) a
                       ) a;"""
                for row in self._connection.execute(sql):
                    table = {k:v for k,v in dict(row).items() if v is not None}
                    k = table.pop('table_name')
                    table['rows'] = rows[k]
                    stats[k] = table
            finally:
                transaction.commit()
        return stats

    def get_consumer_groups(self):
        Groups = self._meta.tables['read_queue_groups']
        Queue = self._meta.tables['read_queue']
        query = select(
             [
                Groups.c.group_name,
                func.count(Queue.c.alert_ids).label('chunks'),
                func.sum(func.array_length(Queue.c.alert_ids,1)).label('items'),
             ]
        ).select_from(Groups.outerjoin(Queue)).group_by(Groups.c.group_id).order_by(Groups.c.group_id)
        return self._connection.execute(query).fetchall()

    def remove_consumer_group(self, pattern):
        Groups = self._meta.tables['read_queue_groups']
        Queue = self._meta.tables['read_queue']
        self._connection.execute(Groups.delete().where(Groups.c.group_name.like(pattern)))

    def _populate_read_queue(self, group_id, block_size, condition, order):
        Queue = self._meta.tables['read_queue']
        numbered = select(
            [
                self._alert_id_column,
                func.row_number().over(order_by=order).label('row_number')
            ]
        ).where(condition).alias('numbered')
        alert_id, row_number = numbered.columns
        block = func.div(row_number-1, block_size)
        self._connection.execute(
            Queue.insert().from_select(
                [Queue.c.group_id, Queue.c.alert_ids],
                select(
                    [
                        group_id,
                        func.array_agg(alert_id)
                    ]
                ).group_by(block).order_by(block)
            )
        )
        col = Queue.c.alert_ids
        return self._connection.execute(select(
            [
                func.count(col).label('chunks'),
                func.sum(func.array_length(col,1)).label('items')
            ]
        ).where(Queue.c.group_id==group_id)).fetchone()

    def _fetch_alerts_with_condition(
        self, condition, order=None, with_history=False, with_cutouts=False,
        group_name=None, block_size=None
    ):
        """
        """

        if group_name is not None:
            Groups = self._meta.tables['read_queue_groups']
            Queue = self._meta.tables['read_queue']
            with self._connection.begin() as transaction:
                try:
                    # Create the group. This will raise IntegrityError if the
                    # group already exists
                    result = self._connection.execute(
                        Groups.insert(),
                        group_name=group_name
                    )
                    group_id = result.inserted_primary_key[0]
                    # Populate the group queue in the same transaction
                    queue_info = self._populate_read_queue(group_id, block_size, condition, order)
                    transaction.commit()
                    log.info("Created group {} with id {} ({} items in {} chunks)".format(group_name, group_id, queue_info['items'], queue_info['chunks']))
                except IntegrityError as e:
                    # If we arrive here, then another client already committed
                    # the group name and populated the queue.
                    transaction.rollback()
                    group_id = self._connection.execute(
                        select([Groups.c.group_id])
                        .where(Groups.c.group_name==group_name)
                    ).fetchone()[0]
                    chunks = self._connection.execute(select(
                        [func.count(Queue.c.alert_ids).label('chunks')]
                    ).where(Queue.c.group_id==group_id)).fetchone()[0]
                    log.info("Subscribed to group {} with id {} ({} chunks remaining)".format(group_name, group_id, chunks))
                except Exception as e:
                    log.error(e)
                    raise
            # Pop a block of alert IDs from the queue that is not already
            # locked by another client, and lock it for the duration of the
            # transaction.
            item_id = Queue.c.item_id
            popped_item = Queue.delete().where(
                Queue.c.item_id == \
                    select([item_id]) \
                        .where(Queue.c.group_id==group_id) \
                        .order_by(item_id.asc()) \
                        .with_for_update(skip_locked=True) \
                        .limit(1)
            ).returning(Queue.c.alert_ids).cte()
            # Query for alerts whose IDs were in the block
            alert_query = self._build_alert_query(
                self._alert_id_column.in_(select([func.unnest(popped_item.c.alert_ids)])),
                None,
                with_history,
                with_cutouts,
            )
        else:
            alert_query = self._build_alert_query(
                condition, 
                order, 
                with_history,
                with_cutouts
            )

        while True:
            nrows = 0
            with self._connection.begin() as transaction:
                for result in self._connection.execute(alert_query):
                    nrows += 1
                    yield self._apply_schema(result)
                # If we reach this point, all alerts from the block have been
                # consumed. Commit the transaction to delete the queue item. If
                # the generator is destroyed before we reach this point,
                # however, the transaction will be rolled back, releasing the
                # lock on the queue item and allowing another client to claim
                # it.
                transaction.commit()
            # If in shared queue mode (group_name is not None), execute
            # the query over and over again until it returns no rows.
            # If in standalone mode (group name is None), execute it
            # only once
            if nrows == 0 or group_name is None:
                break
            else:
                chunks = self._connection.execute(select(
                    [func.count(Queue.c.alert_ids).label('chunks')]
                ).where(Queue.c.group_id==group_id)).fetchone()[0]
                log.info("Query complete after {} alerts, {} chunks remaining".format(nrows, chunks))

    def _build_alert_query(self, condition, order=None, with_history=True, with_cutouts=False):
        """
        Build a query whose results _mostly_ match the structure of the orginal
        alert packet.
        """
        from sqlalchemy.sql.expression import func, literal_column, true
        from sqlalchemy.dialects.postgresql import JSON

        meta = self._meta
        PrvCandidate = meta.tables['prv_candidate']
        UpperLimit = meta.tables['upper_limit']
        Candidate = meta.tables['candidate']
        Alert = meta.tables['alert']
        Pivot = meta.tables['alert_prv_candidate_pivot']
        UpperLimitPivot = meta.tables['alert_upper_limit_pivot']
        Cutout = meta.tables['cutout']

        json_agg = lambda table: func.json_agg(literal_column('"'+ table.name+'"'))

        alert = (
            select([Alert.c.alert_id] + without_keys(Alert))
            .select_from(Alert.join(Candidate))
            .where(condition)
            .order_by(order)
            .alias()
        )

        candidate = (
            select([
                json_agg(Candidate).cast(JSON)[0].label('candidate')
            ])
            .select_from(Candidate)
            .where(Candidate.c.alert_id==alert.c.alert_id)
        )

        query = alert.join(candidate.lateral(), true())

        if with_history:
            # unpack the array of keys from the bridge table in order to perform a normal join
            prv_candidates_ids = select(
                [Pivot.c.alert_id, func.unnest(Pivot.c.prv_candidate_id).label('prv_candidate_id')]
            ).alias()
            prv_candidates = (
                select([
                    json_agg(PrvCandidate).label('prv_candidates')
                ])
                .select_from(
                    PrvCandidate.join(
                        prv_candidates_ids,
                        PrvCandidate.c.prv_candidate_id == prv_candidates_ids.c.prv_candidate_id
                    )
                )
                .where(prv_candidates_ids.c.alert_id==alert.c.alert_id)
            )

            upper_limit_ids = select(
                [UpperLimitPivot.c.alert_id, func.unnest(UpperLimitPivot.c.upper_limit_id).label('upper_limit_id')]
            ).alias()
            upper_limits = (
                select([
                    json_agg(UpperLimit).label('upper_limits')
                ])
                .select_from(
                    UpperLimit.join(
                        upper_limit_ids,
                        UpperLimit.c.upper_limit_id == upper_limit_ids.c.upper_limit_id
                    )
                )
                .where(upper_limit_ids.c.alert_id==alert.c.alert_id)
            )

            query = (
                query
                .join(prv_candidates.lateral(), true())
                .join(upper_limits.lateral(), true())
            )

        if with_cutouts:
            cutout = (
                select([
                    json_agg(Cutout).label('cutouts')
                ])
                .select_from(Cutout)
                .where(Cutout.c.alert_id==alert.c.alert_id)
            )
            query = query.join(cutout.lateral(), true())

        return query.select()

    def _apply_schema(self, candidate_row):
        alert = dict(candidate_row)

        # trim artifacts of schema adaptation
        for k in ("alert_id", "jd", "programid", "partition_id", "ingestion_time"):
            alert.pop(k)
        alert["publisher"] = "Ampel"
        fluff = {"alert_id", "prv_candidate_id", "upper_limit_id"}
        missing = {"programpi", "pdiffimfilename"}
        def schemify(candidate):
            for k in fluff:
                candidate.pop(k, None)
            for k in missing:
                candidate[k] = None
            return candidate

        alert["candidate"] = schemify(alert["candidate"])
        alert["prv_candidates"] = [
            schemify(c)
            for c in sorted(
                ((alert.get("prv_candidates") or []) + (alert.pop("upper_limits", None) or [])),
                key=lambda c: (c["jd"], c.get("candid") is None, c.get("candid"))
            )
        ]

        for cutout in alert.pop("cutouts", None) or []:
            alert[f"cutout{cutout['kind'].title()}"] = {
                'stampData': bytes.fromhex(cutout['stampData'][2:]),
                'fileName': 'unknown'
            }

        return alert

    def _fetch_photopoints_with_condition(
        self, condition
    ):
        """
        Get all photopoints from alerts that match the condition, deduplicating
        history. This can be up to 100x faster than repeated queries for
        individual alerts.
        """
        from sqlalchemy.sql.expression import func, literal_column, union_all

        meta = self._meta
        PrvCandidate = meta.tables['prv_candidate']
        UpperLimit = meta.tables['upper_limit']
        Candidate = meta.tables['candidate']
        Alert = meta.tables['alert']
        Pivot = meta.tables['alert_prv_candidate_pivot']
        UpperLimitPivot = meta.tables['alert_upper_limit_pivot']

        prv_candidate_id = func.unnest(Pivot.c.prv_candidate_id).label('prv_candidate_id')
        prv_candidate_ids = (
            select([Alert.c.objectId, prv_candidate_id])
            .select_from(
                Alert.join(Pivot, isouter=True)
            )
            .where(condition)
            .distinct(prv_candidate_id)
            .alias()
        )
        prv_candidates = PrvCandidate.join(prv_candidate_ids, prv_candidate_ids.c.prv_candidate_id == PrvCandidate.c.prv_candidate_id)

        upper_limit_id = func.unnest(UpperLimitPivot.c.upper_limit_id).label('upper_limit_id')
        upper_limit_ids = (
            select([upper_limit_id])
            .select_from(
                Alert.join(UpperLimitPivot, isouter=True)
            )
            .where(condition)
            .distinct(upper_limit_id)
            .alias()
        )
        upper_limits = UpperLimit.join(upper_limit_ids, upper_limit_ids.c.upper_limit_id == UpperLimit.c.upper_limit_id)

        json_agg = lambda table: func.json_agg(literal_column('"'+ table.name+'"'))
        q = (
            union_all(
                select([json_agg(UpperLimit).label('upper_limits')]).select_from(upper_limits),
                select([json_agg(PrvCandidate).label('prv_candidates')]).select_from(prv_candidates),
                select([json_agg(Candidate).label('candidates')]).select_from(Candidate.join(Alert)).where(condition),
            )
        )

        # ensure exactly one observation per jd. in case of conflicts, sort by
        # candidate > prv_candidate > upper_limit, then pid
        photopoints = dict()
        for row in self._connection.execute(q):
            for pp in sorted(row[0] or [], key=lambda pp: (pp["jd"], pp["pid"])):
                photopoints[pp["jd"]] = pp

        return [photopoints[k] for k in sorted(photopoints.keys(), reverse=True)]

    def count_alerts(self):
        return self._connection.execute(select([func.count(self._alert_id_column)])).fetchone()[0]

    def get_alert(self, candid, with_history=True, with_cutouts=False):
        """
        Retrieve an alert from the archive database
    
        :param candid: `candid` of the alert to retrieve
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images
        :returns: the target alert as a :py:class:`dict`, or `None` if the alert is
                  not in the archive
        """
        Alert = self._meta.tables['alert']

        for alert in self._fetch_alerts_with_condition(
            Alert.c.candid == candid,
            with_history=with_history, with_cutouts=with_cutouts):
            return alert
        return None


    def get_cutout(self, candid):
        """
        """
        Alert = self._meta.tables['alert']
        Cutout = self._meta.tables['cutout']
        q = select(
            [Cutout.c.kind, Cutout.c.stampData]
        ).select_from(
            Cutout.join(Alert)
        ).where(
            Alert.c.candid == candid
        )
        return dict(self._connection.execute(q).fetchall())


    def get_alerts_for_object(
        self, objectId, jd_start=-float('inf'), jd_end=float('inf'), with_history=False, with_cutouts=False
    ):
        """
        Retrieve alerts from the archive database by ID
    
        :param connection: database connection
        :param meta: schema metadata
        :param objectId: id of the transient, e.g. ZTF18aaaaaa, or a collection thereof
        :param jd_start: minimum JD of exposure start
        :param jd_end: maximum JD of exposure start
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images
        :returns: a generator of alerts matching the condition
        """
        Alert = self._meta.tables['alert']
        if isinstance(objectId, str):
            match = Alert.c.objectId == objectId
        elif isinstance(objectId, collections.Collection):
            match = Alert.c.objectId.in_(objectId)
        else:
            raise TypeError("objectId must be str or collection, got {}".format(type(objectId)))
        in_range = and_(Alert.c.jd >= jd_start, Alert.c.jd < jd_end, match)

        yield from self._fetch_alerts_with_condition(
            in_range, Alert.c.jd.asc(),
            with_history=with_history, with_cutouts=with_cutouts
        )


    def get_photopoints_for_object(
        self, objectId, programid=None, jd_start=-float('inf'), jd_end=float('inf')
    ):
        """
        Retrieve unique photopoints from the archive database by object ID.

        :param objectId: id of the transient, e.g. ZTF18aaaaaa, or a collection thereof
        :param jd_start: minimum JD of alert exposure start
        :param jd_end: maximum JD of alert exposure start
        :returns: an alert-packet-like dict containing all returned photopoints
        """
        Alert = self._meta.tables['alert']
        match = Alert.c.objectId == objectId
        in_range = and_(Alert.c.jd >= jd_start, Alert.c.jd < jd_end, match)
        if isinstance(programid, int):
            in_range = and_(in_range, self._get_alert_column('programid') == programid)

        datapoints = self._fetch_photopoints_with_condition(in_range)
        if datapoints:
            candidate = datapoints.pop(0)
            return {
                "objectId": objectId,
                "candid": candidate["candid"],
                "programid": candidate["programid"],
                "candidate": candidate,
                "prv_candidates": datapoints,
            }
        else:
            return None


    def get_alerts(self, candids, with_history=True, with_cutouts=False):
        """
        Retrieve alerts from the archive database by ID
    
        :param alert_id: a collection of `candid` of alerts to retrieve
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images
        :returns: a generator of alerts matching the condition
        """
        Alert = self._meta.tables['alert']
        # mimic mysql field() function, passing the order by hand
        order = sqlalchemy.text(','.join(('alert.candid=%d DESC' % i for i in candids)))

        yield from self._fetch_alerts_with_condition(Alert.c.candid.in_(candids), order,
            with_history=with_history, with_cutouts=with_cutouts)


    def get_alerts_in_time_range(
        self, jd_min, jd_max, programid=None, with_history=True, with_cutouts=False,
        group_name=None, block_size=5000,
    ):
        """
        Retrieve a range of alerts from the archive database

        :param jd_start: minimum JD of exposure start
        :param jd_end: maximum JD of exposure start
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images
        :param group_name: consumer group name. This is used to partition the
            results of the query among clients in the same group.
        :param block_size: partition results in chunks with this many alerts
        """
        jd = self._get_alert_column('jd')
        in_range = and_(jd >= jd_min, jd < jd_max)

        if isinstance(programid, int):
            in_range = and_(in_range, self._get_alert_column('programid') == programid)

        yield from self._fetch_alerts_with_condition(
            in_range, jd.asc(),
            with_history=with_history, with_cutouts=with_cutouts,
            group_name=group_name, block_size=block_size,
        )


    def get_alerts_in_cone(
        self, ra, dec, radius, jd_min=None, jd_max=None, with_history=False, with_cutouts=False
    ):
        """
        Retrieve a range of alerts from the archive database

        :param ra: right ascension of search field center in degrees (J2000)
        :param dec: declination of search field center in degrees (J2000)
        :param radius: radius of search field in degrees
        :param jd_start: minimum JD of exposure start
        :param jd_end: maximum JD of exposure start
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images
        
        """
        from sqlalchemy import func
        from sqlalchemy.sql.expression import BinaryExpression
        Alert = self._meta.tables['alert']
        Candidate = self._meta.tables['candidate']
    
        center = func.ll_to_earth(dec, ra)
        box = func.earth_box(center, radius)
        loc = func.ll_to_earth(Candidate.c.dec, Candidate.c.ra)
    
        in_range = and_(BinaryExpression(box, loc, '@>'), func.earth_distance(center, loc) < radius)
        # NB: filtering on jd from Candidate here is ~2x faster than _also_
        #      filtering on Alert (rows that pass are joined on the indexed
        #      primary key)
        if jd_min is not None:
            in_range = and_(in_range, Candidate.c.jd >= jd_min)
        if jd_max is not None:
            in_range = and_(in_range, Candidate.c.jd < jd_max)

        yield from self._fetch_alerts_with_condition(
            in_range, Alert.c.jd.asc(),
            with_history=with_history, with_cutouts=with_cutouts
        )

def consumer_groups_command() -> None:
    from argparse import ArgumentParser
    import json

    from ampel.core import AmpelContext
    from ampel.dev.DictSecretProvider import DictSecretProvider
    from ampel.model.UnitModel import UnitModel

    parser = ArgumentParser(add_help=True)
    parser.add_argument('config_file_path')
    parser.add_argument('--secrets', type=DictSecretProvider.load, required=True)

    subparsers = parser.add_subparsers(help="command help", dest="action")
    subparsers.required = True
    def add_command(name, help=None) -> ArgumentParser:
        p = subparsers.add_parser(name, help=help)
        p.set_defaults(action=name)
        return p

    p = add_command('list', help='list groups')

    p = add_command('remove', help='remove consumer group')
    p.add_argument('group_name', help='Name of consumer group to remove. This may contain SQL wildcards (%,_)')
    p.set_defaults(action='remove')

    args = parser.parse_args()

    ctx = AmpelContext.load(args.config_file_path, secrets=args.secrets)
    assert ctx.loader.secrets is not None

    archive = ArchiveDB(
        ctx.config.get('resource.ampel-ztf/archive', str, raise_exc=True),
        connect_args=ctx.loader.secrets.get(
            f"ztf/archive/{'reader' if args.action=='list' else 'writer'}",
            dict,
        ).get()
    )
    if args.action == 'remove':
        archive.remove_consumer_group(args.group_name)
    print(json.dumps(list(map(dict,archive.get_consumer_groups())), indent=1)) # pylint: disable=bad-builtin

