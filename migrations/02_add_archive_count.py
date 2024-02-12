#!/usr/bin/env python

import logging
import time
from argparse import ArgumentParser
import sqlalchemy as sa
import fastavro

import boto3

from ampel.ztf.t0.ArchiveUpdater import ArchiveUpdater
from ampel.ztf.archive.server.s3 import get_object, get_key_for_url, get_stream


logging.basicConfig(level="INFO", format="[%(asctime)s] %(message)s")
log = logging.getLogger()

parser = ArgumentParser()
parser.add_argument("uri")
parser.add_argument("--s3-bucket")
parser.add_argument("--s3-endpoint")


args = parser.parse_args()

engine = sa.engine.create_engine(args.uri)

bucket = boto3.resource("s3", endpoint_url=args.s3_endpoint).Bucket(args.s3_bucket)

with engine.connect() as connection:
    meta = sa.MetaData(connection)
    meta.reflect()

    Alert = meta.tables["alert"]
    Archive = meta.tables["avro_archive"]

    update = Archive.update(
        Archive.c.avro_archive_id == sa.bindparam("id"),
        values={"count": sa.bindparam("count")},
    )

    for row in connection.execute(Archive.select(Archive.c.count == None)).fetchall():
        key = get_key_for_url(bucket, row["uri"])
        count = len(list(fastavro.reader(get_stream(bucket, key))))

        connection.execute(update, id=row["avro_archive_id"], count=count)

        print(row, count)
