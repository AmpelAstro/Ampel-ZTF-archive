#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/pipeline/t0/ZUDSArchiveUpdater.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 08.01.2020
# Last Modified Date: 08.10.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from ampel.ztf.archive.ArchiveDBClient import ArchiveDBClient
from sqlalchemy import UniqueConstraint, select, bindparam
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import tuple_, func
from distutils.version import LooseVersion

class ZUDSArchiveUpdater(ArchiveDBClient):
    """
    """

    def insert_alert(self, alert, *args, **kwargs):
        """
        Insert an alert into the archive database
    
        :param alert: alert dict
        """
        if LooseVersion(alert['schemavsn']) > self._alert_version:
            raise ValueError(
                "alert schema ({}) is newer than database schema ({})".format(
                    alert['schemavsn'], self._alert_version)
            )

        Candidate = self._meta.tables['candidate']
        Photopoint = self._meta.tables['photopoint']
        Bridge = self._meta.tables['alert_photopoint_pivot']

        with self._connection.begin() as transaction:
            try:
                self._connection.execute(
                    Candidate.insert(),
                    objectId=alert['objectId'],
                    schemavsn=alert['schemavsn'],
                    **alert['candidate']
                )
            except IntegrityError:
                # abort on duplicate alerts
                transaction.rollback()
                return False

            # insert cutouts if they exist
            prefix = 'cutout'
            cutouts = [
                dict(kind=k[len(prefix):].lower(), stampData=v, candid=alert['candid']) 
                for k,v in alert.items() if k.startswith(prefix) if v is not None
            ]
            if cutouts:
                self._connection.execute(self._meta.tables['cutout'].insert(), cutouts)

            if alert['light_curve']:
                # insert rows if needed
                self._connection.execute(postgresql.insert(Photopoint).on_conflict_do_nothing(), alert['light_curve'])

                # build a condition that selects the rows (just inserted or already existing)
                # identifiers = next(filter(lambda c: isinstance(c, UniqueConstraint), Photopoint.constraints)).columns
                # keys = [[r[c.name] for c in identifiers] for r in alert['light_curve']]
                # target = tuple_(*identifiers).in_(keys)
                target = Photopoint.columns.id.in_([pp['id'] for pp in alert['light_curve']])

                # collect the ids of the rows in an array and insert into the bridge table
                source = select(
                    [
                        bindparam('candid'), 
                        func.array_agg(
                            Photopoint.columns['id']
                        )
                    ]
                ).where(target)

                self._connection.execute(
                    Bridge.insert().from_select(Bridge.columns, source), 
                    candid=alert['candid']
                )

            transaction.commit()
        return True
