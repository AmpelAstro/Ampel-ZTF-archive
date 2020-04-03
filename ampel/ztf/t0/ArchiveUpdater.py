#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/t0/ArchiveUpdater.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 10.04.2018
# Last Modified Date: 14.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.ztf.archive.ArchiveDBClient import ArchiveDBClient
from sqlalchemy import UniqueConstraint, select, bindparam
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import tuple_, func
from distutils.version import LooseVersion

class ArchiveUpdater(ArchiveDBClient):
    """
    """

    def insert_alert(self, alert, schema, partition_id, ingestion_time):
        """
        Insert an alert into the archive database
    
        :param alert: alert dict
        :param schema: avro schema dictionary
        :param partition_id: the index of the Kafka partition this alert came from
        :param ingestion_time: time the alert was received, in UNIX epoch microseconds
        """
        if LooseVersion(schema['version']) > self._alert_version:
            raise ValueError(
                "alert schema ({}) is newer than database schema ({})".format(
                    schema['version'], self._alert_version)
            )

        with self._connection.begin() as transaction:
            Alert = self._meta.tables['alert']
            Candidate = self._meta.tables['candidate']
            try:
                result = self._connection.execute(
                    Alert.insert(),
                    programid=alert['candidate']['programid'], jd=alert['candidate']['jd'],
                    partition_id=partition_id, 
                    ingestion_time=ingestion_time, 
                    **alert
                )
                alert_id = result.inserted_primary_key[0]
                self._connection.execute(
                    Candidate.insert(), alert_id=alert_id, **alert['candidate']
                )
            except IntegrityError:
                # abort on duplicate alerts
                transaction.rollback()
                return False

            # insert cutouts if they exist
            prefix = 'cutout'
            cutouts = [
                dict(kind=k[len(prefix):].lower(), stampData=v['stampData'], alert_id=alert_id) 
                for k,v in alert.items() if k.startswith(prefix) if v is not None
            ]
            if len(cutouts) > 0:
                self._connection.execute(self._meta.tables['cutout'].insert(), cutouts)
    
            if alert['prv_candidates'] is None or len(alert['prv_candidates']) == 0:
                return True
    
            # entries in prv_candidates will often be duplicated, but may also
            # be updated without warning. sort these into detections (which
            # come with unique ids) and upper limits (which don't)
            detections = []
            upper_limits = []
            for index, c in enumerate(alert['prv_candidates']):
                # entries with no candid are nondetections
                if c['candid'] is None:
                    upper_limits.append(c)
                else:
                    detections.append(c)
            for rows, label in ((detections, 'prv_candidate'), (upper_limits, 'upper_limit')):
                if len(rows) > 0:
                    self._update_history(label, rows, alert_id)

            transaction.commit()
        return True


    def _update_history(self, label, rows, alert_id):
        """
        """
        # insert the rows if needed
        history = self._meta.tables[label]
        self._connection.execute(postgresql.insert(history).on_conflict_do_nothing(), rows)

        # build a condition that selects the rows (just inserted or already existing)
        identifiers = next(filter(lambda c: isinstance(c, UniqueConstraint), history.constraints)).columns
        keys = [[r[c.name] for c in identifiers] for r in rows]
        target = tuple_(*identifiers).in_(keys)

        # collect the ids of the rows in an array and insert into the bridge table
        bridge = self._meta.tables['alert_{}_pivot'.format(label)]
        source = select(
            [
                bindparam('alert_id'), 
                func.array_agg(
                    history.columns['{}_id'.format(label)]
                )
			]
        ).where(target)

        self._connection.execute(
            bridge.insert().from_select(bridge.columns, source), 
            alert_id=alert_id
        )
