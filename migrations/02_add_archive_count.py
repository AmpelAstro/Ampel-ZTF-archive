#!/usr/bin/env python

import logging
from argparse import ArgumentParser

import boto3
import fastavro
import sqlalchemy as sa

from ampel.ztf.archive.server.s3 import get_key_for_url, get_stream

logging.basicConfig(level="INFO", format="[%(asctime)s] %(message)s")
log = logging.getLogger()

parser = ArgumentParser()
parser.add_argument("uri")
parser.add_argument("--s3-bucket")
parser.add_argument("--s3-endpoint")
parser.add_argument("--dry-run", default=False, action="store_true")
parser.add_argument("--check-bucket", default=False, action="store_true")


args = parser.parse_args()

engine = sa.engine.create_engine(args.uri)

bucket = boto3.resource("s3", endpoint_url=args.s3_endpoint).Bucket(args.s3_bucket)

with engine.connect() as connection:
    meta = sa.MetaData(connection)
    meta.reflect()

    Alert = meta.tables["alert"]
    Archive = meta.tables["avro_archive"]

    update_count = Archive.update(
        Archive.c.avro_archive_id == sa.bindparam("id"),
        values={"count": sa.bindparam("count")},
    )

    update_refcount = Archive.update(
        Archive.c.avro_archive_id == sa.bindparam("id"),
        values={"refcount": sa.select([sa.func.count(Alert.c.avro_archive_id)])},
    )

    log.info("filling in missing avro_archive.count")
    for row in connection.execute(Archive.select(Archive.c.count is None)).fetchall():
        key = get_key_for_url(bucket, row["uri"])
        count = len(list(fastavro.reader(get_stream(bucket, key))))

        connection.execute(update_count, id=row["avro_archive_id"], count=count)

        print(row, count)

    log.info("filling in missing avro_archive.refcount")
    target = (
        sa.select(
            [
                Alert.c.avro_archive_id,
                sa.func.count(Alert.c.avro_archive_id).label("refcount"),
            ]
        )
        .where(
            Alert.c.avro_archive_id.in_(
                sa.select([Archive.c.avro_archive_id]).where(Archive.c.refcount == 0)
            )
        )
        .group_by(Alert.c.avro_archive_id)
    ).alias("target")
    connection.execute(
        Archive.update()
        .values(refcount=target.c.refcount)
        .where(Archive.c.avro_archive_id == target.c.avro_archive_id)
    )

    log.info("cleaning up blobs not referenced by any alert")
    with connection.begin() as transaction:
        i = 0
        for i, row in enumerate(
            connection.execute(
                Archive.delete(
                    sa.and_(
                        Archive.c.refcount == 0,
                        Archive.c.avro_archive_id
                        < (
                            # ignore last 100 objects to avoid a race condition
                            # where an avro_archive entry is inserted, but not
                            # yet referenced
                            sa.select([Alert.c.avro_archive_id - 100])
                            .order_by(Alert.c.alert_id.desc())
                            .limit(1)
                        ),
                    )
                ).returning(Archive.c.uri)
            ).fetchall(),
            1,
        ):
            key = get_key_for_url(bucket, row["uri"])
            if args.dry_run:
                log.info(bucket.Object(key).delete())
            else:
                log.info(f"would delete {key}")
        log.info(f"Deleted {i} objects not referenced by any alert row")
        if args.dry_run:
            transaction.rollback()
        else:
            transaction.commit()

    if args.check_bucket:
        log.info("finding blobs not referenced in avro_archive")
        in_bucket = {summary.key: summary.size for summary in bucket.objects.all()}
        log.info(
            f"{bucket.name}: {sum(in_bucket.values()) / 2**30:.1f} GB in {len(in_bucket)} items"
        )
        in_archive = {
            get_key_for_url(bucket, row["uri"]): row["avro_archive_id"]
            for row in connection.execute(
                sa.select([Archive.c.avro_archive_id, Archive.c.uri]).where(
                    Archive.c.refcount > 0
                )
            ).fetchall()
        }
        missing_in_bucket = sorted(
            in_archive[key] for key in set(in_archive.keys()).difference(in_bucket)
        )
        missing_in_archive = [key for key in set(in_bucket).difference(in_archive)]
        log.info(f"{len(missing_in_bucket)} missing in bucket: {missing_in_bucket}")
        log.info(f"{len(missing_in_archive)} missing in archive {missing_in_archive}")

        for key in missing_in_archive:
            if args.dry_run:
                log.info(f"would delete {key}")
            else:
                log.info(bucket.Object(key).delete())
        log.info(
            f"Deleted {len(missing_in_archive)} objects not referenced by any avro_archive row"
        )
    else:
        log.warning("skipped bucket check; enable with --check-bucket")
