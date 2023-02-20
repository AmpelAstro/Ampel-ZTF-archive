#!/usr/bin/env python

from argparse import ArgumentParser
import sqlalchemy as sa
from tqdm import tqdm

from ampel.ztf.t0.ArchiveUpdater import ArchiveUpdater
from astropy_healpix import lonlat_to_healpix
from astropy import units as u

parser = ArgumentParser()
parser.add_argument("uri")

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
                Candidate.columns._hpx.is_(None),
                Candidate.c.candidate_id > sa.bindparam("min_id"),
            )
        )
        .order_by(Candidate.c.candidate_id)
        .limit(1000)
    )
    update = (
        sa.update(Candidate)
        .where(Candidate.c.candidate_id == sa.bindparam("id"))
        .values(_hpx=sa.bindparam("hpx"))
    )

    total = connection.execute("select reltuples as estimate from pg_class where relname = 'candidate';").fetchone()[0]

    min_id = -1
    updated = 0
    with tqdm(total=total) as progress:
        while True:
            with connection.begin() as transaction:
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

                transaction.commit()
            progress.update(len(rows))
