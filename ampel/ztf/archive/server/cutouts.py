import io
import json
import pathlib
from typing import Any, BinaryIO

import fastavro
from fastavro._read_py import BLOCK_READERS, BinaryDecoder

with open(pathlib.Path(__file__).parent / "cutout_schema.json") as f:
    CUTOUT_SCHEMA = json.load(f)

ALERT_SCHEMAS: dict[tuple[str, str], Any] = {}


def get_parsed_schema(schema: dict):
    key = schema["name"], schema["version"]
    if key not in ALERT_SCHEMAS:
        ALERT_SCHEMAS[key] = fastavro.parse_schema(schema)
    return ALERT_SCHEMAS[key]


def read_schema(fo: BinaryIO) -> dict[str, Any]:
    reader = fastavro.reader(fo)
    assert isinstance(reader.writer_schema, dict)
    return reader.writer_schema


def repack_alert(alert: dict) -> bytes:
    """
    Reduce an IPAC alert packet to cutouts only. This reduces the packet size by a factor ~1.7.
    NB: cutouts are already gzipped FITS, so further compression is unhelpful.
    """
    with io.BytesIO() as fo:
        fastavro.writer(
            fo,
            CUTOUT_SCHEMA,
            [
                {
                    k: v
                    for k, v in alert.items()
                    if k.startswith("cutout") or k == "candid"
                }
            ],
        )
        return fo.getvalue()


def pack_records(
    records: list[dict], schema: dict, codec: str = "null"
) -> tuple[bytes, list[tuple[int, int]]]:
    """
    Pack a block of alerts into a single Avro payload, returning a list of byte ranges for each
    """
    buf = io.BytesIO()
    fastavro.writer(buf, get_parsed_schema(schema), records, codec=codec)
    buf.seek(0)

    # read back to extract the offsets of each block
    ranges = []
    reader = fastavro.block_reader(buf)
    # now that the header has been consumed, note the offset
    pos = buf.tell()
    for block in reader:
        # reader has consumed the block; note offset
        end = buf.tell()
        for _ in block:
            ranges.append((pos, end))  # noqa: PERF401
        pos = end

    return buf.getvalue(), ranges


def extract_alert(
    candid: int, block: BinaryIO, schema: dict, codec: str = "null"
) -> dict[str, Any]:
    """
    Extract single record from Avro block
    """
    read_block = BLOCK_READERS.get(codec)
    if read_block is None:
        raise KeyError(f"Unknown codec {codec}")

    decoder = BinaryDecoder(block)
    # consume record count to advance to the compressed block
    nrecords = decoder.read_long()
    assert nrecords > 0
    # consume compressed block
    buf = read_block(decoder)
    # iterate over deserialized records
    for _ in range(nrecords):
        alert = fastavro.schemaless_reader(buf, get_parsed_schema(schema), None)
        if isinstance(alert, dict) and alert["candid"] == candid:
            return alert
    raise KeyError(f"{candid} not found in block")
