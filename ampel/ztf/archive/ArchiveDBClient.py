#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/ztf/archive/ArchiveDBClient.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 10.04.2018
# Last Modified Date: 14.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from functools import cached_property

from sqlalchemy import MetaData, create_engine, select
from sqlalchemy.exc import SAWarning

from distutils.version import LooseVersion
import logging
import warnings

class ArchiveDBClient:
    """
    """

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize and connect to archive database. Arguments will be passed on
        to :py:func:`sqlalchemy.create_engine`.
        """
        logging.getLogger('sqlalchemy').setLevel(logging.ERROR)
        self._engine = create_engine(*args, **kwargs)
    
    @cached_property
    def _alert_version(self) -> LooseVersion:
        Versions = self._meta.tables['versions']
        with self._engine.connect() as conn:
            return LooseVersion(
                conn.execute(
                    select(
                        [Versions.c.alert_version]
                    ).order_by(
                        Versions.c.version_id.desc()
                    ).limit(1)
                ).first()[0]
            )

    @cached_property
    def _meta(self) -> MetaData:
        meta = MetaData()
        with warnings.catch_warnings():
            # we know that sqlalchemy can't reflect earthdistance indexes
            warnings.simplefilter("ignore", category=SAWarning)
            meta.reflect(bind=self._engine)
        return meta



