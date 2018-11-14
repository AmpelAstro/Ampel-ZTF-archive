#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/archive/ArchiveDBClient.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 10.04.2018
# Last Modified Date: 14.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from sqlalchemy import MetaData, create_engine, select
from distutils.version import LooseVersion

class ArchiveDBClient:
    """
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize and connect to archive database. Arguments will be passed on
        to :py:func:`sqlalchemy.create_engine`.
        """
        engine = create_engine(*args, **kwargs)
        self._meta = MetaData()
        self._meta.reflect(bind=engine)
        self._connection = engine.connect()

        Versions = self._meta.tables['versions']
        with self._connection.begin() as transaction:
            try:
                self._alert_version = LooseVersion(
                    self._connection.execute(
                        select(
                            [Versions.c.alert_version]
                        ).order_by(
                            Versions.c.version_id.desc()
                        ).limit(1)
                    ).first()[0]
                )
            finally:
                transaction.commit()
