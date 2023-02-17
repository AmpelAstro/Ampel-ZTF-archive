#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/archive/ArchiveDB.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 10.04.2018
# Last Modified Date: 02.12.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import datetime
import itertools
import json
import math
import operator
from typing import (
    Any,
    Dict,
    Mapping,
    Tuple,
    Optional,
    List,
    TYPE_CHECKING,
    Union,
    cast,
    TypedDict,
)
from contextlib import contextmanager

from sqlalchemy import select, update, and_, or_
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.elements import (
    BooleanClauseList,
    ClauseElement,
    ColumnClause,
    UnaryExpression,
)
from sqlalchemy.sql.expression import func
from sqlalchemy.sql.schema import Column, Table
from sqlalchemy.exc import IntegrityError
import sqlalchemy, collections

from ampel.ztf.archive.ArchiveDBClient import ArchiveDBClient
from .types import FilterClause

if TYPE_CHECKING:
    from sqlalchemy.sql.visitors import Visitable

import logging

log = logging.getLogger("ampel.ztf.archive")


def without_keys(table, filter=lambda name: True):
    keys = set(table.primary_key.columns)
    for fk in table.foreign_keys:
        keys.update(fk.constraint.columns)
    return [c for c in table.columns if c not in keys and filter(c.name)]


class GroupNotFoundError(KeyError):
    ...


class NoSuchColumnError(KeyError):
    ...


class ChunkCount(TypedDict):
    chunks: int
    items: int


class GroupInfo(TypedDict):
    error: Optional[bool]
    msg: Optional[str]
    chunk_size: int
    remaining: ChunkCount
    pending: ChunkCount
    started_at: datetime.datetime
    finished_at: Optional[datetime.datetime]


class ArchiveDB(ArchiveDBClient):
    """ """

    _CLIENTS: Dict[str, "ArchiveDB"] = {}

    # ~3 arcsec
    nside = 1<<16

    query_debug = False

    def __init__(self, *args, default_statement_timeout: int = 60000, **kwargs):
        """ """
        super().__init__(*args, **kwargs)
        self._alert_id_column = self.get_alert_id_column()
        self._table_mapping: dict[str, str] = {}
        self._default_statement_timeout = default_statement_timeout

    @contextmanager
    def connect(self):
        with self._engine.connect() as conn:
            conn.execute(f"set statement_timeout={self._default_statement_timeout}")
            yield conn

    def _get_alert_column(self, name: str) -> ColumnClause:
        if "alert" in self._meta.tables and name in self._meta.tables["alert"].c:
            return getattr(self._meta.tables["alert"].c, name)
        else:
            return getattr(self.get_table("candidate").c, name)

    def get_alert_id_column(self):
        return self._meta.tables["alert"].c.alert_id

    def get_table(self, name: str) -> Table:
        return self._meta.tables[self._table_mapping.get(name, name)]

    @classmethod
    def instance(cls, *args, **kwargs) -> "ArchiveDB":
        """
        Get a shared instance of a client with the given connection parameters
        """
        key = json.dumps({"args": args, "kwargs": kwargs})
        if not key in cls._CLIENTS:
            cls._CLIENTS[key] = cls(*args, **kwargs)
        return cls._CLIENTS[key]

    def get_statistics(self) -> Dict[str, Any]:
        """ """
        stats = {}
        with self.connect() as conn:
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
                table = {k: v for k, v in dict(row).items() if v is not None}
                k: str = table.pop("table_name")
                table["rows"] = rows[k]
                stats[k] = table
        return stats

    def get_consumer_groups(self) -> List[Dict[str, Any]]:
        Groups = self._meta.tables["read_queue_groups"]
        Queue = self._meta.tables["read_queue"]
        query = (
            select(
                [
                    Groups.c.group_name,
                    func.count(Queue.c.alert_ids).label("chunks"),
                    func.sum(func.array_length(Queue.c.alert_ids, 1)).label("items"),
                ]
            )
            .select_from(Groups.outerjoin(Queue))
            .group_by(Groups.c.group_id)
            .order_by(Groups.c.group_id)
        )
        with self.connect() as conn:
            return conn.execute(query).fetchall()

    def create_topic(
        self, name: str, candidate_ids: List[int], description: Optional[str] = None
    ) -> int:
        Groups = self._meta.tables["topic_groups"]
        Queue = self._meta.tables["topic"]
        Alert = self._meta.tables["alert"]
        # TODO: chunk inputs, add update operation
        with self.connect() as conn:
            result = conn.execute(
                Groups.insert(),
                topic_name=name,
                topic_description=description,
            )
            topic_id = result.inserted_primary_key[0]
            alert_id = (
                select([Alert.c.alert_id])
                .where(Alert.c.candid.in_(candidate_ids))
                .alias()
                .c.alert_id
            )
            conn.execute(
                Queue.insert().from_select(
                    [Queue.c.topic_id, Queue.c.alert_ids],
                    select([topic_id, func.array_agg(alert_id)]),
                )
            )
        return topic_id

    def get_topic_info(self, topic: str) -> Dict[str, Any]:
        Topic = self._meta.tables["topic"]
        TopicGroups = self._meta.tables["topic_groups"]
        with self.connect() as conn:
            if (
                row := conn.execute(
                    select(
                        [TopicGroups.c.topic_id, TopicGroups.c.topic_description]
                    ).where(TopicGroups.c.topic_name == topic)
                ).fetchone()
            ) is None:
                raise GroupNotFoundError
            else:
                topic_id: int = row.topic_id

            info = conn.execute(
                select([func.sum(func.array_length(Topic.c.alert_ids, 1))]).where(
                    Topic.c.topic_id == topic_id
                )
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
    ) -> Dict[str, Any]:
        Groups = self._meta.tables["read_queue_groups"]
        Queue = self._meta.tables["read_queue"]
        Topic = self._meta.tables["topic"]
        TopicGroups = self._meta.tables["topic_groups"]
        with self.connect() as conn:
            if (
                row := conn.execute(
                    select([TopicGroups.c.topic_id]).where(
                        TopicGroups.c.topic_name == topic
                    )
                ).fetchone()
            ) is None:
                raise GroupNotFoundError
            else:
                topic_id: int = row.topic_id
            group_id = conn.execute(
                Groups.insert(),
                group_name=group_name,
                chunk_size=block_size,
            ).inserted_primary_key[0]

            unnested = (
                select([func.unnest(Topic.c.alert_ids).label("alert_id")])
                .where(Topic.c.topic_id == topic_id)
                .alias()
            )
            numbered = select(
                [
                    unnested.c.alert_id,
                    func.row_number().over(order_by="alert_id").label("row_number"),
                ]
            ).alias()
            alert_id, row_number = numbered.columns
            if selection != slice(None):
                conditions = []
                # NB: row_number is 1-indexed
                if selection.start is not None:
                    conditions.append(row_number > selection.start)
                if selection.stop is not None:
                    conditions.append(row_number <= selection.stop)
                if selection.step is not None:
                    conditions.append((row_number - 1) % selection.step == 0)
                where: Visitable = and_(*conditions)
            else:
                where = sqlalchemy.true()

            block = func.div(
                row_number - (1 + (selection.start or 0)),
                block_size * (selection.step or 1),
            )

            q = Queue.insert().from_select(
                [Queue.c.group_id, Queue.c.alert_ids],
                select([group_id, func.array_agg(alert_id)])
                .where(where)
                .group_by(block)
                .order_by(block),
            )

            conn.execute(q)

            conn.execute(Groups.update(Groups.c.group_id == group_id, {"error": False}))
            col = Queue.c.alert_ids
            return conn.execute(
                select(
                    [
                        func.count(col).label("chunks"),
                        func.sum(func.array_length(col, 1)).label("items"),
                    ]
                ).where(Queue.c.group_id == group_id)
            ).fetchone()

    def remove_consumer_group(self, pattern: str) -> None:
        Groups = self._meta.tables["read_queue_groups"]
        Queue = self._meta.tables["read_queue"]
        with self.connect() as conn:
            return conn.execute(
                Groups.delete().where(Groups.c.group_name.like(pattern))
            )

    def _select_alert_ids(
        self, group_id, block_size, condition, order: List[UnaryExpression] = []
    ):
        """Generate a statement that selects blocks of alert_id"""

        # harvest tables involved in condition
        tables = set()
        sqlalchemy.sql.visitors.traverse(
            condition,
            visitors={"column": lambda column: tables.add(column.table)},
            opts={},
        )
        # join alert table only if needed
        if self._alert_id_column.table in tables:
            alert_id = self._alert_id_column
            source = self._alert_id_column.table.join(self.get_table("candidate"))
        else:
            alert_id = self.get_table("candidate").c.alert_id
            source = alert_id.table
        numbered = (
            select(
                [
                    alert_id,
                    func.row_number().over(order_by=order).label("row_number"),
                ]
            )
            .select_from(source)
            .where(condition)
            .alias("numbered")
        )
        alert_ids, row_number = numbered.columns
        block = func.div(row_number - 1, block_size)
        return (
            select([group_id, func.array_agg(alert_ids)])
            .group_by(block)
            .order_by(block)
        )

    def _populate_read_queue(
        self, conn, group_id, block_size, condition, order: List[UnaryExpression] = []
    ):
        Queue = self._meta.tables["read_queue"]
        blocks = self._select_alert_ids(group_id, block_size, condition, order)
        if self.query_debug:
            log.warn(
                str(
                    blocks.compile(
                        dialect=sqlalchemy.dialects.postgresql.dialect(),
                        compile_kwargs={"literal_binds": True},
                    )
                )
            )
        conn.execute(
            Queue.insert().from_select(
                [Queue.c.group_id, Queue.c.alert_ids],
                blocks,
            )
        )
        col = Queue.c.alert_ids
        return conn.execute(
            select(
                [
                    func.count(col).label("chunks"),
                    func.sum(func.array_length(col, 1)).label("items"),
                ]
            ).where(Queue.c.group_id == group_id)
        ).fetchone()

    def _create_read_queue(
        self,
        conn: Connection,
        condition: BooleanClauseList,
        order: List[UnaryExpression],
        group_name: str,
        block_size: int,
    ) -> Tuple[int, int]:

        Groups = self._meta.tables["read_queue_groups"]

        result = conn.execute(
            Groups.insert(), group_name=group_name, chunk_size=block_size
        )
        group_id = result.inserted_primary_key[0]
        try:
            queue_info = self._populate_read_queue(
                conn, group_id, block_size, condition, order
            )
            conn.execute(
                Groups.update(
                    Groups.columns.group_id == group_id,
                    values={"error": False, "resolved": func.now()},
                )
            )

            chunks = queue_info["chunks"]
            log.info(
                "Created group {} with id {} ({} items in {} chunks)".format(
                    group_name, group_id, queue_info["items"], queue_info["chunks"]
                )
            )
            return group_id, chunks
        except sqlalchemy.exc.SQLAlchemyError as exc:
            conn.execute(
                Groups.update(
                    Groups.columns.group_id == group_id,
                    values={"error": True, "msg": str(exc)},
                )
            )
            log.exception(f"Failed to create group {group_name}")
            return group_id, 0

    def get_group_info(self, group_name: str) -> GroupInfo:
        Groups = self._meta.tables["read_queue_groups"]
        Queue = self._meta.tables["read_queue"]
        with self.connect() as conn:
            if (
                row := conn.execute(
                    select(
                        [
                            Groups.c.group_id,
                            Groups.c.chunk_size,
                            Groups.c.error,
                            Groups.c.msg,
                            Groups.c.created,
                            Groups.c.resolved
                        ]
                    ).where(Groups.c.group_name == group_name)
                ).fetchone()
            ) is not None:
                group_id: int = row["group_id"]
                chunk_size: int = row["chunk_size"]
                error: bool = row["error"]
                msg: Optional[str] = row["msg"]
                created: datetime.datetime = row["created"]
                resolved: Optional[datetime.datetime] = row["resolved"]
            else:
                raise GroupNotFoundError
            col = Queue.c.alert_ids
            pending = (Queue.c.issued != sqlalchemy.sql.null()).label("pending")
            info: GroupInfo = {
                "error": error,
                "msg": msg,
                "chunk_size": chunk_size,
                "remaining": {"chunks": 0, "items": 0},
                "pending": {"chunks": 0, "items": 0},
                "started_at": created,
                "finished_at": resolved,
            }
            for row in conn.execute(
                select(
                    [
                        pending,
                        func.count(col).label("chunks"),
                        func.sum(func.array_length(col, 1)).label("items"),
                    ]
                )
                .where(Queue.c.group_id == group_id)
                .group_by(pending)
            ):
                target = info["pending"] if row["pending"] else info["remaining"]
                target["chunks"] = row["chunks"]
                target["items"] = row["items"]

            return info

    def _fetch_alerts_with_condition(
        self,
        conn,
        condition,
        order: List[UnaryExpression] = [],
        *,
        distinct: bool = False,
        with_history=False,
        group_name=None,
        block_size=None,
        max_blocks=None,
        limit: Optional[int] = None,
        skip: int = 0,
    ) -> tuple[int, list[dict[str, Any]]]:
        """ """
        if group_name is not None:
            if distinct:
                # TODO
                raise NotImplementedError(
                    "latest sorting is not implemented for queues"
                )
            group_id, _ = self._create_read_queue(
                conn, condition, order, group_name, block_size
            )
            return self.get_chunk_from_queue(group_name, with_history=with_history)

        alert_query = self._build_alert_query(
            condition,
            order=order,
            distinct=distinct,
            with_history=with_history,
            limit=limit,
            skip=skip,
        )

        alerts = []
        while True:
            nrows = 0
            nchunks = 0
            with conn.begin() as transaction:
                for result in conn.execute(alert_query):
                    nrows += 1
                    alerts.append(self._apply_schema(result))
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
            if (
                nrows == 0
                or group_name is None
                or (max_blocks is not None and nchunks >= max_blocks)
            ):
                break
        return -1, alerts

    def get_chunk_from_queue(self, group_name: str, with_history: bool = True):
        Groups = self._meta.tables["read_queue_groups"]
        Queue = self._meta.tables["read_queue"]
        with self.connect() as conn:
            # Update the access time to keep the group from being deleted
            # after 24 hours of inactivity.
            group_id = (
                result[0]
                if (
                    result := conn.execute(
                        update(Groups)
                        .where(Groups.c.group_name == group_name)
                        .values(last_accessed=func.now())
                        .returning(Groups.c.group_id)
                    ).fetchone()
                )
                is not None
                else None
            )
            # Fail gracefully on nonexistant groups
            if group_id is None:
                raise GroupNotFoundError
            # Pop a block of alert IDs from the queue that is not already
            # locked by another client, and lock it for the duration of the
            # transaction.
            item_id = Queue.c.item_id
            popped_item = (
                Queue.update()
                .where(
                    Queue.c.item_id
                    == select([item_id])
                    .where(Queue.c.group_id == group_id)
                    .where(Queue.c.issued == sqlalchemy.sql.null())
                    .order_by(item_id.asc())
                    .with_for_update(skip_locked=True)
                    .limit(1)
                )
                .values(issued=sqlalchemy.func.now())
                .returning(Queue.c.item_id)
                .cte()
            )
            # Query for alerts whose IDs were in the block
            alert_ids = select([func.unnest(Queue.c.alert_ids)]).where(
                Queue.c.item_id == popped_item.c.item_id
            )
            alert_query = self._build_alert_query(
                self._alert_id_column.in_(alert_ids),
                with_history=with_history,
            )
            alert_query.append_column(popped_item.c.item_id.label("_chunk_id"))

            alerts = []
            chunk_id = None
            for row in conn.execute(alert_query):
                chunk_id = row["_chunk_id"]
                alerts.append(self._apply_schema(row))

            return chunk_id, alerts

    def acknowledge_chunk_from_queue(self, group_name: str, item_id: int):
        Groups = self._meta.tables["read_queue_groups"]
        Queue = self._meta.tables["read_queue"]
        with self.connect() as conn:
            conn.execute(
                Queue.delete()
                .where(Queue.c.group_id == Groups.c.group_id)
                .where(Groups.c.group_name == group_name)
                .where(Queue.c.item_id == item_id)
            )

    def release_chunk_from_queue(self, group_name: str, item_id: int):
        Groups = self._meta.tables["read_queue_groups"]
        Queue = self._meta.tables["read_queue"]
        with self.connect() as conn:
            conn.execute(
                Queue.update()
                .where(Queue.c.group_id == Groups.c.group_id)
                .where(Groups.c.group_name == group_name)
                .where(Queue.c.item_id == item_id)
                .values(issued=sqlalchemy.sql.null)
            )

    def _build_base_alert_query(
        self,
        columns: List[Column],
        condition,
        *,
        order: List[UnaryExpression] = [],
        distinct: bool = False,
        limit: Optional[int] = None,
        skip: int = 0,
    ):
        meta = self._meta
        Candidate = self.get_table("candidate")
        Alert = meta.tables["alert"]

        alert_base = (
            select(columns)
            .select_from(
                Alert.join(Candidate, Alert.c.alert_id == Candidate.c.alert_id)
            )
            .where(condition)
            .order_by(*(([Alert.c.objectId.asc()] if distinct else []) + order))
        )
        if limit is not None:
            alert_base = alert_base.limit(limit).offset(skip)
        if distinct:
            return alert_base.distinct(Alert.c.objectId)
        else:
            return alert_base

    def _build_alert_query(
        self,
        condition,
        *,
        order: List[UnaryExpression] = [],
        distinct: bool = False,
        with_history: bool = True,
        limit: Optional[int] = None,
        skip: int = 0,
    ):
        """
        Build a query whose results _mostly_ match the structure of the orginal
        alert packet.
        """
        from sqlalchemy.sql.expression import func, literal_column, true
        from sqlalchemy.dialects.postgresql import JSON

        meta = self._meta
        PrvCandidate = meta.tables["prv_candidate"]
        UpperLimit = meta.tables["upper_limit"]
        Candidate = self.get_table("candidate")
        Alert = meta.tables["alert"]
        Pivot = meta.tables["alert_prv_candidate_pivot"]
        UpperLimitPivot = meta.tables["alert_upper_limit_pivot"]

        json_agg = lambda table: func.json_agg(literal_column('"' + table.name + '"'))

        alert = self._build_base_alert_query(
            [Alert.c.alert_id]
            + without_keys(Alert, lambda k: not k.startswith("avro_archive_")),
            condition,
            order=order,
            distinct=distinct,
            limit=limit,
            skip=skip,
        ).alias()

        candidate = (
            select([json_agg(Candidate).cast(JSON)[0].label("candidate")])
            .select_from(Candidate)
            .where(Candidate.c.alert_id == alert.c.alert_id)
        )

        query = alert.join(candidate.lateral(), true())

        if with_history:
            # unpack the array of keys from the bridge table in order to perform a normal join
            prv_candidates_ids = select(
                [
                    Pivot.c.alert_id,
                    func.unnest(Pivot.c.prv_candidate_id).label("prv_candidate_id"),
                ]
            ).alias()
            prv_candidates = (
                select([json_agg(PrvCandidate).label("prv_candidates")])
                .select_from(
                    PrvCandidate.join(
                        prv_candidates_ids,
                        PrvCandidate.c.prv_candidate_id
                        == prv_candidates_ids.c.prv_candidate_id,
                    )
                )
                .where(prv_candidates_ids.c.alert_id == alert.c.alert_id)
            )

            upper_limit_ids = select(
                [
                    UpperLimitPivot.c.alert_id,
                    func.unnest(UpperLimitPivot.c.upper_limit_id).label(
                        "upper_limit_id"
                    ),
                ]
            ).alias()
            upper_limits = (
                select([json_agg(UpperLimit).label("upper_limits")])
                .select_from(
                    UpperLimit.join(
                        upper_limit_ids,
                        UpperLimit.c.upper_limit_id == upper_limit_ids.c.upper_limit_id,
                    )
                )
                .where(upper_limit_ids.c.alert_id == alert.c.alert_id)
            )

            query = query.join(prv_candidates.lateral(), true()).join(
                upper_limits.lateral(), true()
            )

        return query.select()

    def _apply_schema(self, candidate_row):
        alert = dict(candidate_row)

        # trim artifacts of schema adaptation
        for k in (
            "alert_id",
            "jd",
            "programid",
            "partition_id",
            "ingestion_time",
            "_chunk_id",
        ):
            alert.pop(k, None)
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
                (
                    (alert.get("prv_candidates") or [])
                    + (alert.pop("upper_limits", None) or [])
                ),
                key=lambda c: (c["jd"], c.get("candid") is None, c.get("candid")),
            )
        ]

        for cutout in alert.pop("cutouts", None) or []:
            alert[f"cutout{cutout['kind'].title()}"] = {
                "stampData": bytes.fromhex(cutout["stampData"][2:]),
                "fileName": "unknown",
            }

        return alert

    def _fetch_photopoints_with_condition(
        self, conn, condition, include_upper_limits: bool = True
    ):
        """
        Get all photopoints from alerts that match the condition, deduplicating
        history. This can be up to 100x faster than repeated queries for
        individual alerts.
        """
        from sqlalchemy.sql.expression import func, literal_column, union_all

        meta = self._meta
        PrvCandidate = meta.tables["prv_candidate"]
        UpperLimit = meta.tables["upper_limit"]
        Candidate = meta.tables["candidate"]
        Alert = meta.tables["alert"]
        Pivot = meta.tables["alert_prv_candidate_pivot"]
        UpperLimitPivot = meta.tables["alert_upper_limit_pivot"]

        json_agg = lambda table: func.json_agg(literal_column('"' + table.name + '"'))

        prv_candidate_id = func.unnest(Pivot.c.prv_candidate_id).label(
            "prv_candidate_id"
        )
        prv_candidate_ids = (
            select([Alert.c.objectId, prv_candidate_id])
            .select_from(Alert.join(Pivot, isouter=True))
            .where(condition)
            .distinct(prv_candidate_id)
            .alias()
        )
        prv_candidates = PrvCandidate.join(
            prv_candidate_ids,
            prv_candidate_ids.c.prv_candidate_id == PrvCandidate.c.prv_candidate_id,
        )

        selects = [
            select([json_agg(PrvCandidate).label("prv_candidates")]).select_from(
                prv_candidates
            ),
            select([json_agg(Candidate).label("candidates")])
            .select_from(Candidate.join(Alert))
            .where(condition),
        ]

        if include_upper_limits:
            upper_limit_id = func.unnest(UpperLimitPivot.c.upper_limit_id).label(
                "upper_limit_id"
            )
            upper_limit_ids = (
                select([upper_limit_id])
                .select_from(Alert.join(UpperLimitPivot, isouter=True))
                .where(condition)
                .distinct(upper_limit_id)
                .alias()
            )
            upper_limits = UpperLimit.join(
                upper_limit_ids,
                upper_limit_ids.c.upper_limit_id == UpperLimit.c.upper_limit_id,
            )
            selects.insert(
                0,
                select([json_agg(UpperLimit).label("upper_limits")]).select_from(
                    upper_limits
                ),
            )

        q = union_all(*selects)

        if self.query_debug:
            log.warn(
                str(
                    q.compile(
                        dialect=sqlalchemy.dialects.postgresql.dialect(),
                        compile_kwargs={"literal_binds": True},
                    )
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
        with self.connect() as conn:
            return conn.execute(select([func.count(self._alert_id_column)])).fetchone()[
                0
            ]

    def get_alert(self, candid: int, *, with_history: bool = True):
        """
        Retrieve an alert from the archive database

        :param candid: `candid` of the alert to retrieve
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images
        :returns: the target alert as a :py:class:`dict`, or `None` if the alert is
                  not in the archive
        """
        Alert = self._meta.tables["alert"]

        with self.connect() as conn:
            chunk_id, alerts = self._fetch_alerts_with_condition(
                conn,
                Alert.c.candid == candid,
                with_history=with_history,
            )
            if alerts:
                return alerts[0]
        return None

    def get_archive_segment(self, candid):
        """ """
        Alert = self._meta.tables["alert"]
        AvroArchive = self._meta.tables["avro_archive"]
        q = (
            select(
                [
                    AvroArchive.c.uri,
                    Alert.c.avro_archive_start,
                    Alert.c.avro_archive_end,
                ]
            )
            .select_from(
                Alert.join(
                    AvroArchive,
                    AvroArchive.c.avro_archive_id == Alert.c.avro_archive_id,
                )
            )
            .where(Alert.c.candid == candid)
        )
        with self.connect() as conn:
            return conn.execute(q).fetchone()

    def get_alerts_for_object(
        self,
        objectId: str,
        *,
        jd_start: Optional[float] = None,
        jd_end: Optional[float] = None,
        programid: Optional[int] = None,
        with_history: bool = False,
        limit: Optional[int] = None,
        start: int = 0,
    ):
        """
        Retrieve alerts from the archive database by ID

        :param connection: database connection
        :param meta: schema metadata
        :param objectId: id of the transient, e.g. ZTF18aaaaaa, or a collection thereof
        :param jd_start: minimum JD of exposure start
        :param jd_end: maximum JD of exposure start
        :param with_history: return alert with previous detections and upper limits
        :returns: a generator of alerts matching the condition
        """
        Alert = self._meta.tables["alert"]
        if isinstance(objectId, str):
            match = Alert.c.objectId == objectId
        elif isinstance(objectId, collections.Collection):
            match = Alert.c.objectId.in_(objectId)
        else:
            raise TypeError(
                "objectId must be str or collection, got {}".format(type(objectId))
            )
        conditions = [match]
        if jd_end is not None:
            conditions.insert(0, Alert.c.jd < jd_end)
        if jd_start is not None:
            conditions.insert(0, Alert.c.jd >= jd_start)
        if programid is not None:
            conditions.insert(0, Alert.c.programid == programid)
        in_range = and_(*conditions)

        with self.connect() as conn:
            return self._fetch_alerts_with_condition(
                conn,
                in_range,
                [Alert.c.alert_id.asc()],
                with_history=with_history,
                limit=limit,
                skip=start,
            )

    def get_photopoints_for_object(
        self,
        objectId: str,
        *,
        programid: Optional[int] = None,
        jd_start: Optional[float] = None,
        jd_end: Optional[float] = None,
        include_upper_limits: bool = True,
    ):
        """
        Retrieve unique photopoints from the archive database by object ID.

        :param objectId: id of the transient, e.g. ZTF18aaaaaa, or a collection thereof
        :param jd_start: minimum JD of alert exposure start
        :param jd_end: maximum JD of alert exposure start
        :returns: an alert-packet-like dict containing all returned photopoints
        """
        Alert = self._meta.tables["alert"]
        match = Alert.c.objectId == objectId
        conditions = [match]
        if jd_end is not None:
            conditions.insert(0, Alert.c.jd < jd_end)
        if jd_start is not None:
            conditions.insert(0, Alert.c.jd >= jd_start)
        if isinstance(programid, int):
            conditions.append(self._get_alert_column("programid") == programid)
        in_range = and_(*conditions)

        with self.connect() as conn:
            datapoints = self._fetch_photopoints_with_condition(
                conn, in_range, include_upper_limits=include_upper_limits
            )
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

    def get_alerts(
        self,
        candids: List[int],
        *,
        with_history: bool = True,
    ):
        """
        Retrieve alerts from the archive database by ID

        :param alert_id: a collection of `candid` of alerts to retrieve
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images
        :returns: a generator of alerts matching the condition
        """
        Alert = self._meta.tables["alert"]
        # mimic mysql field() function, passing the order by hand
        order = cast(
            UnaryExpression,
            sqlalchemy.text(",".join(("alert.candid=%d DESC" % i for i in candids))),
        )

        with self.connect() as conn:
            return self._fetch_alerts_with_condition(
                conn,
                Alert.c.candid.in_(candids),
                [order],
                with_history=with_history,
            )

    def _candidate_filter_condition(
        self, candidate_filter: FilterClause
    ) -> BooleanClauseList:
        Candidate = self.get_table("candidate")
        ops = {
            "$eq": operator.eq,
            "$ne": operator.ne,
            "$gt": operator.gt,
            "$gte": operator.ge,
            "$lt": operator.lt,
            "$lte": operator.le,
            "$in": sqlalchemy.Column.in_,
            "$nin": sqlalchemy.Column.notin_,
        }
        conditions = []
        for column_name, opspec in candidate_filter.items():
            if column_name not in Candidate.c:
                raise NoSuchColumnError(column_name)
            for op, value in opspec.items():
                conditions.append(ops[op](Candidate.c[column_name], value))
        return and_(*conditions)

    def _time_range_condition(
        self,
        programid: Optional[int] = None,
        jd_start: Optional[float] = None,
        jd_end: Optional[float] = None,
        candidate_filter: Optional[FilterClause] = None,
    ) -> Tuple[BooleanClauseList, List[UnaryExpression]]:
        jd = self._get_alert_column("jd")
        Candidate = self.get_table("candidate")

        conditions: List[ClauseElement] = []
        if jd_end is not None:
            conditions.insert(0, jd < jd_end)
        if jd_start is not None:
            conditions.insert(0, jd >= jd_start)
        if programid is not None:
            conditions.insert(0, self._get_alert_column("programid") == programid)
        if candidate_filter is not None:
            conditions.append(self._candidate_filter_condition(candidate_filter))

        return and_(*conditions), [jd.asc()]

    def get_alerts_in_time_range(
        self,
        *,
        jd_start: float,
        jd_end: float,
        programid: Optional[int] = None,
        candidate_filter: Optional[FilterClause] = None,
        with_history: bool = True,
        group_name: str = None,
        block_size: int = 5000,
        max_blocks: Optional[int] = None,
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
        condition, order = self._time_range_condition(
            programid, jd_start, jd_end, candidate_filter
        )

        with self.connect() as conn:
            return self._fetch_alerts_with_condition(
                conn,
                condition,
                order,
                with_history=with_history,
                group_name=group_name,
                block_size=block_size,
                max_blocks=max_blocks,
            )

    def _cone_search_condition(
        self,
        *,
        ra: float,
        dec: float,
        radius: float,
        programid: Optional[int] = None,
        jd_min: Optional[float] = None,
        jd_max: Optional[float] = None,
        latest: bool = False,
        candidate_filter: Optional[FilterClause] = None,
    ) -> Tuple[BooleanClauseList, List[UnaryExpression]]:
        from sqlalchemy import func, or_
        from .server.healpix_cone_search import ranges_for_cone

        Alert = self._meta.tables["alert"]
        Candidate = self.get_table("candidate")

        center = func.ll_to_earth(dec, ra)
        loc = func.ll_to_earth(Candidate.c.dec, Candidate.c.ra)

        pix = Candidate.c._hpx

        nside, ranges = ranges_for_cone(ra, dec, radius, max_nside=self.nside)
        scale = (self.nside // nside)**2
        conditions = [
            or_(
                *[
                    and_(pix >= left*scale, pix < right*scale)
                    for left, right in ranges
                ]
            ),
            func.earth_distance(center, loc) < radius,
        ]
        # NB: filtering on jd from Candidate here is ~2x faster than _also_
        #      filtering on Alert (rows that pass are joined on the indexed
        #      primary key)
        if jd_min is not None:
            conditions.insert(0, Candidate.c.jd >= jd_min)
        if jd_max is not None:
            conditions.insert(0, Candidate.c.jd < jd_max)
        if isinstance(programid, int):
            conditions.insert(0, self._get_alert_column("programid") == programid)
        if candidate_filter is not None:
            conditions.append(self._candidate_filter_condition(candidate_filter))

        return and_(*conditions), [Candidate.c.jd.asc()] if not latest else [
            Candidate.c.jd.desc(),
            Candidate.c.candid.desc(),  # NB: reprocessed points may have identical jds; break ties with candid
        ]

    def _healpix_search_condition(
        self,
        *,
        pixels: Mapping[int, Union[int, List[int]]],
        jd_min: float,
        jd_max: float,
        latest: bool = False,
        programid: Optional[int] = None,
        candidate_filter: Optional[FilterClause] = None,
    ) -> Tuple[BooleanClauseList, List[UnaryExpression]]:
        from .server.skymap import multirange
        Candidate = self.get_table("candidate")

        # NB: searches against sets of pixel indices are reasonably efficient for nside=64,
        # but target lists could become unfeasibly long for large nside. use multiranges
        # with postgres >= 14 for a more efficient search in this case.
        # NBB: expressing range checks as an giant OR clause can be horribly inefficient if
        # if the pixel index is an expression, as postgres does not elide multiple evaluations
        # of healpix_ang2ipix_nest, even though it is marked IMMUTABLE. this can result in
        # e.g. the pixel index being calculated 300 times for every row if you have 300
        # elements in your OR clause, which can end up dominating the runtime of the query.

        # use stored healpix column
        pix = Candidate.c._hpx

        ranges: multirange[int] = multirange()

        for nside, ipix in pixels.items():
            if not (nside <= self.nside and nside >= 1 and math.log2(nside).is_integer()):
                raise ValueError(f"nside must be >= 1, <= {self.nside}, and a power of 2")
            scale = (self.nside // nside) ** 2
            for i in ([ipix] if isinstance(ipix, int) else ipix):
                ranges.add(i*scale, (i+1)*scale)

        and_conditions = [
            or_(
                *[
                    and_(pix >= left, pix < right)
                    for left, right in ranges
                ]
            ),
            Candidate.c.jd >= jd_min,
            Candidate.c.jd < jd_max,
        ]
        if programid is not None:
            and_conditions.append(Candidate.c.programid == programid)
        if candidate_filter is not None:
            and_conditions.append(self._candidate_filter_condition(candidate_filter))

        return (
            and_(*and_conditions),
            [Candidate.c.jd.asc()]
            if not latest
            else [Candidate.c.jd.desc(), Candidate.c.candid.desc()],
        )

    def _object_search_condition(
        self,
        objectId: Union[str, List[str]],
        programid: Optional[int] = None,
        jd_start: Optional[float] = None,
        jd_end: Optional[float] = None,
        candidate_filter: Optional[FilterClause] = None,
    ) -> Tuple[BooleanClauseList, List[UnaryExpression]]:

        Alert = self._meta.tables["alert"]
        if isinstance(objectId, str):
            match = Alert.c.objectId == objectId
        elif isinstance(objectId, collections.Collection):
            match = Alert.c.objectId.in_(objectId)
        else:
            raise TypeError(
                "objectId must be str or collection, got {}".format(type(objectId))
            )
        conditions = [match]

        jd = self._get_alert_column("jd")
        Candidate = self.get_table("candidate")

        if jd_end is not None:
            conditions.insert(0, jd < jd_end)
        if jd_start is not None:
            conditions.insert(0, jd >= jd_start)
        if programid is not None:
            conditions.insert(0, self._get_alert_column("programid") == programid)
        if candidate_filter is not None:
            conditions.append(self._candidate_filter_condition(candidate_filter))

        return and_(*conditions), [jd.asc()]

    def get_alerts_in_cone(
        self,
        *,
        ra: float,
        dec: float,
        radius: float,
        programid: Optional[int] = None,
        jd_start: Optional[float] = None,
        jd_end: Optional[float] = None,
        candidate_filter: Optional[FilterClause] = None,
        latest: bool = False,
        with_history: bool = False,
        group_name: Optional[str] = None,
        block_size: int = 5000,
        max_blocks: Optional[int] = None,
    ):
        """
        Retrieve a range of alerts from the archive database

        :param ra: right ascension of search field center in degrees (J2000)
        :param dec: declination of search field center in degrees (J2000)
        :param radius: radius of search field in degrees
        :param jd_start: minimum JD of exposure start
        :param jd_end: maximum JD of exposure start
        :param latest: return only the latest alert for each objectId
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images

        """
        condition, orders = self._cone_search_condition(
            ra=ra,
            dec=dec,
            radius=radius,
            programid=programid,
            jd_min=jd_start,
            jd_max=jd_end,
            latest=latest,
            candidate_filter=candidate_filter,
        )

        with self.connect() as conn:
            return self._fetch_alerts_with_condition(
                conn,
                condition,
                orders,
                with_history=with_history,
                distinct=latest,
                group_name=group_name,
                block_size=block_size,
                max_blocks=max_blocks,
            )

    def get_objects_in_cone(
        self,
        *,
        ra: float,
        dec: float,
        radius: float,
        programid: Optional[int] = None,
        candidate_filter: Optional[FilterClause] = None,
        jd_start: Optional[float] = None,
        jd_end: Optional[float] = None,
    ):
        """
        Retrieve the set of objectIds with alerts in the given cone

        :param ra: right ascension of search field center in degrees (J2000)
        :param dec: declination of search field center in degrees (J2000)
        :param radius: radius of search field in degrees
        :param programid: programid to query
        :param jd_start: minimum JD of exposure start
        :param jd_end: maximum JD of exposure start

        """
        condition, orders = self._cone_search_condition(
            ra=ra,
            dec=dec,
            radius=radius,
            programid=programid,
            jd_min=jd_start,
            jd_max=jd_end,
            candidate_filter=candidate_filter,
            latest=True,
        )

        with self.connect() as conn:
            for row in conn.execute(
                self._build_base_alert_query(
                    [self._meta.tables["alert"].c.objectId],
                    condition,
                    order=orders,
                    distinct=True,
                )
            ):
                yield row["objectId"]

    def get_alerts_in_healpix(
        self,
        *,
        pixels: Dict[int, Union[int, List[int]]],
        jd_start: float,
        jd_end: float,
        latest: bool = False,
        programid: Optional[int] = None,
        candidate_filter: Optional[FilterClause] = None,
        with_history: bool = False,
        group_name: Optional[str] = None,
        block_size: int = 5000,
        max_blocks: Optional[int] = None,
    ):
        """
        Retrieve alerts by HEALpix pixel

        :param pixels: dict of nside->pixel index (nested ordering)
        :param jd_start: minimum JD of exposure start
        :param jd_end: maximum JD of exposure start
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images

        """
        condition, orders = self._healpix_search_condition(
            pixels=pixels,
            jd_min=jd_start,
            jd_max=jd_end,
            latest=latest,
            programid=programid,
            candidate_filter=candidate_filter,
        )

        with self.connect() as conn:
            return self._fetch_alerts_with_condition(
                conn,
                condition,
                orders,
                distinct=latest,
                with_history=with_history,
                group_name=group_name,
                block_size=block_size,
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

    p = add_command("list", help="list groups")

    p = add_command("remove", help="remove consumer group")
    p.add_argument(
        "group_name",
        help="Name of consumer group to remove. This may contain SQL wildcards (%,_)",
    )
    p.set_defaults(action="remove")

    args = parser.parse_args()

    archive = ArchiveDB(
        args.uri, connect_args={"user": args.user, "password": args.password}
    )
    if args.action == "remove":
        archive.remove_consumer_group(args.group_name)
    print(
        json.dumps(
            list(map(lambda v: dict(v), archive.get_consumer_groups())), indent=1
        )
    )  # pylint: disable=bad-builtin
