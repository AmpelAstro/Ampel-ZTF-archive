#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/archive/ArchiveDB.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 10.04.2018
# Last Modified Date: 02.12.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import json
from typing import Any, Dict, Tuple, Optional, List

from sqlalchemy import select, update, and_, bindparam
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.elements import BooleanClauseList, ClauseElement, ColumnClause, UnaryExpression
from sqlalchemy.sql.expression import func
from sqlalchemy.exc import IntegrityError
import sqlalchemy, collections

from ampel.ztf.archive.ArchiveDBClient import ArchiveDBClient

import logging
log = logging.getLogger('ampel.ztf.archive')

def without_keys(table):
    keys = set(table.primary_key.columns)
    for fk in table.foreign_keys:
        keys.update(fk.constraint.columns)
    return [c for c in table.columns if c not in keys]

class GroupNotFoundError(KeyError):
    ...

class ArchiveDB(ArchiveDBClient):
    """
    """
    _CLIENTS: Dict[str, 'ArchiveDB'] = {}
    def __init__(self, *args, **kwargs):
        """
        """
        super().__init__(*args, **kwargs)
        self._alert_id_column = self.get_alert_id_column()

    def _get_alert_column(self, name: str) -> ColumnClause:
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
        key = json.dumps({"args": args, "kwargs": kwargs})
        if not key in cls._CLIENTS:
            cls._CLIENTS[key] = cls(*args, **kwargs)
        return cls._CLIENTS[key]

    def get_statistics(self) -> Dict[str, Any]:
        """
        """
        stats = {}
        with self._engine.connect() as conn:
            sql = "select relname, n_live_tup from pg_catalog.pg_stat_user_tables"
            rows = dict(conn.execute(sql).fetchall())
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
            for row in conn.execute(sql):
                table = {k:v for k,v in dict(row).items() if v is not None}
                k: str = table.pop('table_name')
                table['rows'] = rows[k]
                stats[k] = table
        return stats

    def get_consumer_groups(self) -> List[Dict[str, Any]]:
        Groups = self._meta.tables['read_queue_groups']
        Queue = self._meta.tables['read_queue']
        query = select(
             [
                Groups.c.group_name,
                func.count(Queue.c.alert_ids).label('chunks'),
                func.sum(func.array_length(Queue.c.alert_ids,1)).label('items'),
             ]
        ).select_from(Groups.outerjoin(Queue)).group_by(Groups.c.group_id).order_by(Groups.c.group_id)
        with self._engine.connect() as conn:
            return conn.execute(query).fetchall()

    def create_topic(self, name: str, candidate_ids: List[int], description: Optional[str]=None) -> int:
        Groups = self._meta.tables['topic_groups']
        Queue = self._meta.tables['topic']
        Alert = self._meta.tables['alert']
        # TODO: chunk inputs, add update operation
        with self._engine.connect() as conn:
            result = conn.execute(
                Groups.insert(),
                topic_name=name,
                topic_description=description,
            )
            topic_id = result.inserted_primary_key[0]
            alert_id = select([Alert.c.alert_id]).where(Alert.c.candid.in_(candidate_ids)).alias().c.alert_id
            conn.execute(
                Queue.insert().from_select(
                    [
                        Queue.c.topic_id,
                        Queue.c.alert_ids
                    ],
                    select(
                        [
                            topic_id,
                            func.array_agg(alert_id)
                        ]
                    )
                )
            )
        return topic_id
    
    def get_topic_info(self, topic: str) -> Dict[str, Any]:
        Topic = self._meta.tables['topic']
        TopicGroups = self._meta.tables['topic_groups']
        with self._engine.connect() as conn:
            if (row := conn.execute(
                select([TopicGroups.c.topic_id, TopicGroups.c.topic_description])
                .where(TopicGroups.c.topic_name==topic)
            ).fetchone()) is None:
                raise GroupNotFoundError
            else:
                topic_id: int = row.topic_id
            
            info = conn.execute(
                select([func.sum(func.array_length(Topic.c.alert_ids, 1))])
                .where(Topic.c.topic_id==topic_id)
            ).fetchone()
            size = 0 if info is None else info[0]
            assert row.topic_description
            return {
                "description": row.topic_description,
                "size": size,
            }      

    def create_read_queue_from_topic(
        self,
        topic: str,
        group_name: str,
        block_size: int,
        selection: slice = slice(None),
    ) -> Dict[str,Any]:
        Groups = self._meta.tables['read_queue_groups']
        Queue = self._meta.tables['read_queue']
        Topic = self._meta.tables['topic']
        TopicGroups = self._meta.tables['topic_groups']
        with self._engine.connect() as conn:
            if (row := conn.execute(
                select([TopicGroups.c.topic_id])
                .where(TopicGroups.c.topic_name==topic)
            ).fetchone()) is None:
                raise GroupNotFoundError
            else:
                topic_id: int = row.topic_id
            group_id = conn.execute(
                Groups.insert(),
                group_name=group_name
            ).inserted_primary_key[0]

            unnested = (
                select([func.unnest(Topic.c.alert_ids).label('alert_id')])
                .where(Topic.c.topic_id==topic_id)
                .alias()
            )
            numbered = select([
                unnested.c.alert_id,
                func.row_number().over(order_by='alert_id').label('row_number')
            ])
            if selection != slice(None):
                r = numbered.c.row_number
                conditions = []
                # NB: row_number is 1-indexed
                if selection.start is not None:
                    conditions.append(r > selection.start)
                if selection.stop is not None:
                    conditions.append(r <= selection.stop)
                if selection.step is not None:
                    conditions.append(r % selection.step == 1)
                alert_id, row_number = numbered.where(and_(*conditions)).alias().columns
            else:
                alert_id, row_number = numbered.alias().columns
            
            block = func.div(row_number-(1+(selection.start or 0)), block_size*(selection.step or 1))
            
            conn.execute(
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
            return conn.execute(select(
                [
                    func.count(col).label('chunks'),
                    func.sum(func.array_length(col,1)).label('items')
                ]
            ).where(Queue.c.group_id==group_id)).fetchone()


    def remove_consumer_group(self, pattern: str) -> None:
        Groups = self._meta.tables['read_queue_groups']
        Queue = self._meta.tables['read_queue']
        with self._engine.connect() as conn:
            return conn.execute(Groups.delete().where(Groups.c.group_name.like(pattern)))

    def _populate_read_queue(self, conn, group_id, block_size, condition, order):
        Queue = self._meta.tables['read_queue']
        numbered = select(
            [
                self._alert_id_column,
                func.row_number().over(order_by=order).label('row_number')
            ]
        ).where(condition).alias('numbered')
        alert_id, row_number = numbered.columns
        block = func.div(row_number-1, block_size)
        conn.execute(
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
        return conn.execute(select(
            [
                func.count(col).label('chunks'),
                func.sum(func.array_length(col,1)).label('items')
            ]
        ).where(Queue.c.group_id==group_id)).fetchone()

    def _create_read_queue(
        self,
        conn: Connection,
        condition: BooleanClauseList,
        order: ClauseElement,
        group_name: str,
        block_size: int
    ) -> Tuple[int, int]:

        Groups = self._meta.tables['read_queue_groups']
        Queue = self._meta.tables['read_queue']
        with conn.begin() as transaction:
            try:
                # Create the group. This will raise IntegrityError if the
                # group already exists
                result = conn.execute(
                    Groups.insert(),
                    group_name=group_name
                )
                group_id = result.inserted_primary_key[0]
                # Populate the group queue in the same transaction
                queue_info = self._populate_read_queue(conn, group_id, block_size, condition, order)
                transaction.commit()
                chunks = queue_info["chunks"]
                log.info("Created group {} with id {} ({} items in {} chunks)".format(group_name, group_id, queue_info['items'], queue_info['chunks']))
            except IntegrityError as e:
                # If we arrive here, then another client already committed
                # the group name and populated the queue.
                transaction.rollback()
                group_id = conn.execute(
                    select([Groups.c.group_id])
                    .where(Groups.c.group_name==group_name)
                ).fetchone()[0]
                chunks = conn.execute(select(
                    [func.count(Queue.c.alert_ids).label('chunks')]
                ).where(Queue.c.group_id==group_id)).fetchone()[0]
                # Update the access time to keep the group from being deleted
                # after 24 hours of inactivity. If another client has already
                # updated the access time, rollback.
                with conn.begin() as transaction:
                    try:
                        conn.execute(
                            update(Groups)
                            .where(Groups.c.group_id==group_id)
                            .values(last_accessed=func.now())
                        )
                    except IntegrityError:
                        transaction.rollback()

                log.info("Subscribed to group {} with id {} ({} chunks remaining)".format(group_name, group_id, chunks))
            except Exception as e:
                log.error(e)
                raise
        return group_id, chunks

    def get_remaining_chunks(self, group_name: str) -> int:
        Groups = self._meta.tables['read_queue_groups']
        Queue = self._meta.tables['read_queue']
        with self._engine.connect() as conn:
            if (row := conn.execute(
                select([Groups.c.group_id])
                .where(Groups.c.group_name==group_name)
            ).fetchone()) is not None:
                group_id: int = row.group_id
            else:
                raise GroupNotFoundError
            return conn.execute(select(
                [func.count(Queue.c.alert_ids).label('chunks')]
            ).where(Queue.c.group_id==group_id)).fetchone()[0]

    def _fetch_alerts_with_condition(
        self, conn, condition, order=None, with_history=False, with_cutouts=False,
        group_name=None, block_size=None, max_blocks=None,
    ):
        """
        """

        if group_name is not None:
            group_id, _ = self._create_read_queue(conn, condition, order, group_name, block_size)
            Queue = self._meta.tables['read_queue']
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
            nchunks = 0
            with conn.begin() as transaction:
                for result in conn.execute(alert_query):
                    nrows += 1
                    yield self._apply_schema(result)
                # If we reach this point, all alerts from the block have been
                # consumed. Commit the transaction to delete the queue item. If
                # the generator is destroyed before we reach this point,
                # however, the transaction will be rolled back, releasing the
                # lock on the queue item and allowing another client to claim
                # it.
                transaction.commit()
                nchunks += 1
            # If in shared queue mode (group_name is not None), execute
            # the query over and over again until it returns no rows.
            # If in standalone mode (group name is None), execute it
            # only once
            if nrows == 0 or group_name is None or (max_blocks is not None and nchunks >= max_blocks):
                break
            else:
                chunks = conn.execute(select(
                    [func.count(Queue.c.alert_ids).label('chunks')]
                ).where(Queue.c.group_id==group_id)).fetchone()[0]
                log.info("Query complete after {} alerts, {} chunks remaining".format(nrows, chunks))

    def get_chunk_from_queue(self, group_name: str, with_history: bool=True, with_cutouts: bool=False):
        Groups = self._meta.tables['read_queue_groups']
        Queue = self._meta.tables['read_queue']
        with self._engine.connect() as conn:
            # TODO could do this with a lateral join
            group_id = (
                result[0]
                if (
                    result := conn.execute(
                        select([Groups.c.group_id])
                        .where(Groups.c.group_name==group_name)
                    ).fetchone()
                ) is not None
                else None
            )
            # Fail gracefully on nonexistant groups
            if group_id is None:
                raise GroupNotFoundError
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
            with conn.begin() as transaction:
                for result in conn.execute(alert_query):
                    yield self._apply_schema(result)
                # If we reach this point, all alerts from the block have been
                # consumed. Commit the transaction to delete the queue item. If
                # the generator is destroyed before we reach this point,
                # however, the transaction will be rolled back, releasing the
                # lock on the queue item and allowing another client to claim
                # it.
                transaction.commit()


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
        self, conn, condition
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
        for row in conn.execute(q):
            for pp in sorted(row[0] or [], key=lambda pp: (pp["jd"], pp["pid"])):
                photopoints[pp["jd"]] = pp

        return [photopoints[k] for k in sorted(photopoints.keys(), reverse=True)]

    def count_alerts(self):
        with self._engine.connect() as conn:
            return conn.execute(select([func.count(self._alert_id_column)])).fetchone()[0]

    def get_alert(self, candid: int, with_history: bool=True, with_cutouts: bool=False):
        """
        Retrieve an alert from the archive database
    
        :param candid: `candid` of the alert to retrieve
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images
        :returns: the target alert as a :py:class:`dict`, or `None` if the alert is
                  not in the archive
        """
        Alert = self._meta.tables['alert']

        with self._engine.connect() as conn:
            for alert in self._fetch_alerts_with_condition(
                conn,
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
        with self._engine.connect() as conn:
            return dict(conn.execute(q).fetchall())


    def get_alerts_for_object(
        self, objectId: str, jd_start: Optional[float]=None, jd_end: Optional[float]=None, with_history: bool=False, with_cutouts: bool=False
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
        conditions = [match]
        if jd_end is not None:
            conditions.insert(0, Alert.c.jd < jd_end)
        if jd_start is not None:
            conditions.insert(0, Alert.c.jd >= jd_start)
        in_range = and_(*conditions)

        with self._engine.connect() as conn:
            yield from self._fetch_alerts_with_condition(
                conn, in_range, Alert.c.jd.asc(),
                with_history=with_history, with_cutouts=with_cutouts
            )


    def get_photopoints_for_object(
        self, objectId: str, programid: Optional[int]=None, jd_start: Optional[float]=None, jd_end: Optional[float]=None
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
        conditions = [match]
        if jd_end is not None:
            conditions.insert(0, Alert.c.jd < jd_end)
        if jd_start is not None:
            conditions.insert(0, Alert.c.jd >= jd_start)
        if isinstance(programid, int):
            conditions.append(self._get_alert_column('programid') == programid)
        in_range = and_(*conditions)

        with self._engine.connect() as conn:
            datapoints = self._fetch_photopoints_with_condition(conn, in_range)
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

        with self._engine.connect() as conn:
            yield from self._fetch_alerts_with_condition(conn, Alert.c.candid.in_(candids), order,
                with_history=with_history, with_cutouts=with_cutouts)

    def _time_range_condition(
        self,
        programid: Optional[int]=None,
        jd_start: Optional[float]=None,
        jd_end: Optional[float]=None,
    ) -> Tuple[BooleanClauseList, UnaryExpression]:
        jd = self._get_alert_column('jd')

        conditions: List[ClauseElement] = []
        if jd_end is not None:
            conditions.insert(0, jd < jd_end)
        if jd_start is not None:
            conditions.insert(0, jd >= jd_start)
        if programid is not None:
            conditions.insert(0, self._get_alert_column('programid') == programid)
    
        return and_(*conditions), jd.asc()

    def get_alerts_in_time_range(
        self, jd_min: float, jd_max: float, programid: Optional[int]=None, with_history: bool=True, with_cutouts: bool=False,
        group_name: str=None, block_size: int=5000, max_blocks: Optional[int]=None
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
        condition, order = self._time_range_condition(programid, jd_min, jd_max)

        with self._engine.connect() as conn:
            yield from self._fetch_alerts_with_condition(
                conn, condition, order,
                with_history=with_history, with_cutouts=with_cutouts,
                group_name=group_name, block_size=block_size, max_blocks=max_blocks,
            )

    def _cone_search_condition(
        self,
        ra: float,
        dec: float,
        radius: float,
        programid: Optional[int]=None,
        jd_min: Optional[float]=None,
        jd_max: Optional[float]=None,
    ) -> Tuple[BooleanClauseList, UnaryExpression]:
        from sqlalchemy import func
        from sqlalchemy.sql.expression import BinaryExpression
        Alert = self._meta.tables['alert']
        Candidate = self._meta.tables['candidate']
    
        center = func.ll_to_earth(dec, ra)
        box = func.earth_box(center, radius)
        loc = func.ll_to_earth(Candidate.c.dec, Candidate.c.ra)
    
        in_range = and_(
            BinaryExpression(box, loc, '@>'), # type: ignore
            func.earth_distance(center, loc) < radius
        )
        # NB: filtering on jd from Candidate here is ~2x faster than _also_
        #      filtering on Alert (rows that pass are joined on the indexed
        #      primary key)
        if jd_min is not None:
            in_range = and_(in_range, Candidate.c.jd >= jd_min)
        if jd_max is not None:
            in_range = and_(in_range, Candidate.c.jd < jd_max)
        if isinstance(programid, int):
            in_range = and_(in_range, self._get_alert_column('programid') == programid)
        
        return in_range, Alert.c.jd.asc()

    def get_alerts_in_cone(
        self,
        ra: float,
        dec: float,
        radius: float,
        programid: Optional[int]=None,
        jd_min: Optional[float]=None,
        jd_max: Optional[float]=None,
        with_history: bool=False,
        with_cutouts: bool=False,
        group_name: Optional[str]=None,
        block_size: int=5000,
        max_blocks: Optional[int]=None,
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
        condition, order = self._cone_search_condition(ra, dec, radius, programid, jd_min, jd_max)

        with self._engine.connect() as conn:
            yield from self._fetch_alerts_with_condition(
                conn, condition, order,
                with_history=with_history, with_cutouts=with_cutouts,
                group_name=group_name, block_size=block_size, max_blocks=max_blocks,
            )

def consumer_groups_command() -> None:
    from argparse import ArgumentParser
    import json

    parser = ArgumentParser(add_help=True)
    parser.add_argument("uri")
    parser.add_argument("--user")
    parser.add_argument("--password")

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

    archive = ArchiveDB(
        args.uri,
        connect_args={"user": args.user, "password": args.password}
    )
    if args.action == 'remove':
        archive.remove_consumer_group(args.group_name)
    print(json.dumps(list(map(lambda v: dict(v),archive.get_consumer_groups())), indent=1)) # pylint: disable=bad-builtin

