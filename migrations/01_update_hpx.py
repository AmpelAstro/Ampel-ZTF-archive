#!/usr/bin/env python

import logging
import time
from argparse import ArgumentParser

import sqlalchemy as sa
from astropy import units as u
from astropy_healpix import lonlat_to_healpix

from ampel.ztf.t0.ArchiveUpdater import ArchiveUpdater

logging.basicConfig(level="INFO", format="[%(asctime)s] %(message)s")
log = logging.getLogger()

parser = ArgumentParser()
parser.add_argument("uri")
parser.add_argument("--chunk-size", type=int, default=1000)
parser.add_argument("--min-id", type=int, default=-1)

args = parser.parse_args()

engine = sa.engine.create_engine(args.uri)

with engine.connect() as connection:
    meta = sa.MetaData(connection)
    meta.reflect()

    Candidate = meta.tables["candidate"]

    select = (
        sa.select([Candidate.c.candidate_id, Candidate.c.ra, Candidate.c.dec])
        .where(
            sa.and_(
                Candidate.columns._hpx.is_(None),  # noqa: SLF001
                Candidate.c.candidate_id > sa.bindparam("min_id"),
            )
        )
        .order_by(Candidate.c.candidate_id)
        .limit(args.chunk_size)
        .with_for_update(skip_locked=True)
    )
    update = (
        sa.update(Candidate)
        .where(Candidate.c.candidate_id == sa.bindparam("id"))
        .values(_hpx=sa.bindparam("hpx"))
    )

    total = connection.execute(
        "select reltuples as estimate from pg_class where relname = 'candidate';"
    ).fetchone()[0]

    min_id = args.min_id
    updated = 0
    while True:
        t0 = time.time()
        rows = connection.execute(select, {"min_id": min_id}).fetchall()

        if len(rows) == 0:
            break

        min_id = rows[-1]["candidate_id"]

        connection.execute(
            update,
            [
                {
                    "id": row["candidate_id"],
                    "hpx": int(
                        lonlat_to_healpix(
                            row["ra"] * u.deg,
                            row["dec"] * u.deg,
                            nside=ArchiveUpdater.NSIDE,
                            order="nested",
                        )
                    ),
                }
                for row in rows
            ],
        )
        dt = time.time() - t0
        log.info(f"updated {len(rows)/dt:.1f} rows/s")
