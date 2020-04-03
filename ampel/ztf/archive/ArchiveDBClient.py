#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/archive/ArchiveDBClient.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 10.04.2018
# Last Modified Date: 14.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from sqlalchemy import MetaData, create_engine, select
from sqlalchemy.exc import SAWarning

from distutils.version import LooseVersion
import logging
import warnings

class ArchiveDBClient:
    """
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize and connect to archive database. Arguments will be passed on
        to :py:func:`sqlalchemy.create_engine`.
        """
        logging.getLogger('sqlalchemy').setLevel(logging.ERROR)
        engine = create_engine(*args, **kwargs)
        self._meta = MetaData()
        with warnings.catch_warnings():
            # we know that sqlalchemy can't reflect earthdistance indexes
            warnings.simplefilter("ignore", category=SAWarning)
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
    def close(self):
        self._connection.close()
