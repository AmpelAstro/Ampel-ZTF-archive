#!/usr/bin/env python
# File              : ampel/ztf/t0/ArchiveUpdater.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 10.04.2018
# Last Modified Date: 14.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from collections.abc import Sequence
from typing import Any

import sqlalchemy
from astropy import units as u
from astropy_healpix import lonlat_to_healpix
from packaging.version import Version
from sqlalchemy import Connection, UniqueConstraint, bindparam, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import func, tuple_

from ampel.ztf.archive.ArchiveDBClient import ArchiveDBClient


class ArchiveUpdater(ArchiveDBClient):
    """ """

    def insert_alert_chunk(
        self,
        alerts: list[dict],
        schema: dict[str, Any],
        archive_uri: str,
        ranges: list[tuple[int, int]],
    ):
        if Version(schema["version"]) > self._alert_version:
            raise ValueError(
                "alert schema ({}) is newer than database schema ({})".format(
                    schema["version"], self._alert_version
                )
            )

        AvroArchive = self._meta.tables["avro_archive"]

        with self._engine.connect() as conn:
            if row := conn.execute(
                select(AvroArchive.c.avro_archive_id).where(
                    AvroArchive.c.uri == archive_uri
                )
            ).fetchone():
                archive_id = row[0]
            else:
                row = conn.execute(
                    AvroArchive.insert()
                    .values(uri=archive_uri, count=len(alerts))
                    .returning(AvroArchive.c.avro_archive_id)
                ).fetchone()
                assert row is not None
                archive_id = row[0]
                conn.commit()

                for alert, span in zip(alerts, ranges):
                    with conn.begin() as transaction:
                        self._insert_alert(
                            conn,
                            alert
                            | {
                                "avro_archive_id": archive_id,
                                "avro_archive_start": span[0],
                                "avro_archive_end": span[1],
                            },
                        )
                        transaction.commit()

    def insert_alert(
        self,
        alert: dict[str, Any],
        schema: dict[str, Any],
        partition_id: int,
        ingestion_time: int,
    ):
        """
        Insert an alert into the archive database

        :param alert: alert dict
        :param schema: avro schema dictionary
        :param partition_id: the index of the Kafka partition this alert came from
        :param ingestion_time: time the alert was received, in UNIX epoch microseconds
        """
        if Version(schema["version"]) > self._alert_version:
            raise ValueError(
                "alert schema ({}) is newer than database schema ({})".format(
                    schema["version"], self._alert_version
                )
            )

        with self._engine.connect() as conn:
            if self._insert_alert(conn, alert, partition_id, ingestion_time):
                conn.commit()
            else:
                conn.rollback()

    def _insert_alert(
        self,
        conn: Connection,
        alert: dict[str, Any],
        partition_id: int = 0,
        ingestion_time: int = 0,
    ):
        """
        Insert an alert into the archive database

        :param alert: alert dict
        :param schema: avro schema dictionary
        :param partition_id: the index of the Kafka partition this alert came from
        :param ingestion_time: time the alert was received, in UNIX epoch microseconds

        """
        Alert = self._meta.tables["alert"]
        Candidate = self._meta.tables["candidate"]

        # add healpix index
        candidate = alert["candidate"]
        candidate["_hpx"] = int(
            lonlat_to_healpix(
                candidate["ra"] * u.deg,
                candidate["dec"] * u.deg,
                self.NSIDE,
                order="nested",
            )
        )

        insert_alert = postgresql.insert(Alert).values(
            programid=alert["candidate"]["programid"],
            jd=alert["candidate"]["jd"],
            partition_id=partition_id,
            ingestion_time=ingestion_time,
            **{k: v for k, v in alert.items() if k in Alert.c},
        )
        avro_archive_fields = {k: v for k, v in alert.items() if k.startswith("avro_")}
        if avro_archive_fields:
            insert_alert = insert_alert.on_conflict_do_update(
                index_elements=("candid", "programid"),
                set_=avro_archive_fields,
            )
        else:
            insert_alert = insert_alert.on_conflict_do_nothing(
                index_elements=("candid", "programid"),
            )

        result = conn.execute(
            insert_alert.returning(
                Alert.c.alert_id,
                (sqlalchemy.column("xmax") == 0).label("inserted"),
            )
        ).fetchone()

        # abort if alert already existed
        if result is None or result[1] is False:
            return False
        alert_id = result[0]

        conn.execute(
            Candidate.insert().values(
                alert_id=alert_id,
                **{
                    k: v
                    for k, v in alert["candidate"].items()
                    if k in Candidate.columns
                },
            )
        )

        # entries in prv_candidates will often be duplicated, but may also
        # be updated without warning. sort these into detections (which
        # come with unique ids) and upper limits (which don't)
        detections = []
        upper_limits = []
        for c in alert["prv_candidates"] or []:
            # entries with no candid are nondetections
            if c["candid"] is None:
                upper_limits.append(c)
            else:
                detections.append(c)
        for rows, label in (
            (detections, "prv_candidate"),
            (upper_limits, "upper_limit"),
        ):
            if len(rows) > 0:
                self._update_history(conn, label, rows, alert_id)

        return True

    def _update_history(
        self,
        conn: Connection,
        label: str,
        rows: Sequence[dict[str, Any]],
        alert_id: int,
    ):
        """ """
        # insert the rows if needed
        history = self._meta.tables[label]
        conn.execute(postgresql.insert(history).on_conflict_do_nothing(), rows)

        # build a condition that selects the rows (just inserted or already existing)
        identifiers = next(
            c for c in history.constraints if isinstance(c, UniqueConstraint)
        ).columns
        keys = [[r[c.name] for c in identifiers] for r in rows]
        target = tuple_(*identifiers).in_(keys)

        # collect the ids of the rows in an array and insert into the bridge table
        bridge = self._meta.tables[f"alert_{label}_pivot"]
        source = select(
            bindparam("alert_id"),
            func.array_agg(history.columns[f"{label}_id"]),
        ).where(target)

        conn.execute(
            bridge.insert().from_select(bridge.columns.keys(), source),
            {"alert_id": alert_id},
        )
