import io
import fastavro
import json
import pathlib
import gzip

with open(pathlib.Path(__file__).parent / "cutout_schema.json") as f:
    CUTOUT_SCHEMA = json.load(f)


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
