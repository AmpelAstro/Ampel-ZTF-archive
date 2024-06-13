import base64
import hashlib
import io
import json
import secrets
from typing import TYPE_CHECKING, Any, Literal, Optional, Union
from urllib.parse import urlsplit

import fastavro
import sqlalchemy
from fastapi import (
    BackgroundTasks,
    Body,
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    status,
)
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic.fields import Field

from ampel.ztf.archive.ArchiveDB import (
    ArchiveDB,
    GroupInfo,
    GroupNotFoundError,
    NoSuchColumnError,
)
from ampel.ztf.archive.server.cutouts import extract_alert, pack_records
from ampel.ztf.archive.server.skymap import deres
from ampel.ztf.t0.ArchiveUpdater import ArchiveUpdater

from .db import (
    OperationalError,
    get_archive,
    get_archive_updater,
    handle_operationalerror,
)
from .models import (
    Alert,
    AlertChunk,
    AlertCount,
    AlertCutouts,
    AlertQuery,
    HEALpixMapQuery,
    HEALpixRegionCountQuery,
    HEALpixRegionQuery,
    ObjectQuery,
    Stream,
    StreamDescription,
    Topic,
    TopicDescription,
    TopicQuery,
)
from .s3 import get_range, get_s3_bucket, get_url_for_key
from .settings import settings
from .tokens import (
    AuthToken,
    verify_access_token,
    verify_write_token,
)
from .tokens import (
    router as token_router,
)

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket


DESCRIPTION = """
Query ZTF alerts issued by IPAC

## Authorization

Some endpoints require an authorization token.
You can create a *ZTF archive access token* using the "Archive tokens" tab on the [Ampel dashboard](https://ampel.zeuthen.desy.de/live/dashboard/tokens).
These tokens are persistent, and associated with your GitHub username.
"""

app = FastAPI(
    title="ZTF Alert Archive Service",
    description=DESCRIPTION,
    version="3.1.0",
    root_path=settings.root_path,
    openapi_tags=[
        {"name": "alerts", "description": "Retrieve alerts"},
        {
            "name": "photopoints",
            "description": "Retrieve de-duplicated detections and upper limits",
        },
        {"name": "cutouts", "description": "Retrieve image cutouts for alerts"},
        {"name": "search", "description": "Search for alerts"},
        {"name": "stream", "description": "Read a result set concurrently"},
        {
            "name": "topic",
            "description": "A topic is a persistent collection of alerts, specified by candidate id. This can be used e.g. to store a pre-selected sample of alerts for analysis.",
        },
        {
            "name": "tokens",
            "description": "Manage persistent authentication tokens",
            "externalDocs": {
                "description": "Authentication dashboard",
                "url": "https://ampel.zeuthen.desy.de/live/dashboard/tokens",
            },
        },
    ],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(token_router, prefix="/tokens")

app.exception_handler(OperationalError)(handle_operationalerror)


# NB: make deserialization depend on write auth to minimize attack surface
async def deserialize_avro_body(
    request: Request, auth: bool = Depends(verify_write_token)
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    data: bytes = await request.body()
    try:
        reader = fastavro.reader(io.BytesIO(data))
    except ValueError as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY, headers={"x-exception": str(exc)}
        )
    try:
        content, schema = list(reader), reader.writer_schema
    except StopIteration as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY, headers={"x-exception": str(exc)}
        )
    return content, schema  # type: ignore[return-value]


@app.post(
    "/alerts",
    tags=["alerts"],
    response_model_exclude_none=True,
)
def post_alert_chunk(
    content_and_schema: tuple[list[dict[str, Any]], dict[str, Any]] = Depends(
        deserialize_avro_body
    ),
    archive: ArchiveUpdater = Depends(get_archive_updater),
    bucket=Depends(get_s3_bucket),
    auth: bool = Depends(verify_write_token),
):
    alerts, schema = content_and_schema

    blob, ranges = pack_records(alerts, schema)
    key = f'{hashlib.sha256(json.dumps(sorted(alert["candid"] for alert in alerts)).encode("utf-8")).hexdigest()}.avro'
    md5 = base64.b64encode(hashlib.md5(blob).digest()).decode("utf-8")

    obj = bucket.Object(key)

    s3_response = obj.put(
        Body=blob,
        ContentMD5=md5,
        Metadata={"schema-name": schema["name"], "schema-version": schema["version"]},
    )
    assert 200 <= s3_response["ResponseMetadata"]["HTTPStatusCode"] < 300

    try:
        archive.insert_alert_chunk(
            alerts, schema, archive_uri=get_url_for_key(bucket, key), ranges=ranges
        )
    except:
        obj.delete()
        raise


def get_alert_from_s3(
    candid: int,
    db: ArchiveDB,
    bucket: "Bucket",
) -> Optional[dict]:
    try:
        uri, start, end = db.get_archive_segment(candid)
    except (ValueError, TypeError):
        return None
    path = urlsplit(uri).path.split("/")
    assert path[-2] == bucket.name
    try:
        return extract_alert(candid, *get_range(bucket, path[-1], start, end))
    except KeyError:
        return None


@app.get(
    "/alert/{candid}",
    tags=["alerts"],
    response_model=Alert,  # type: ignore[arg-type]
    response_model_exclude_none=True,
)
def get_alert(
    candid: int,
    db: ArchiveDB = Depends(get_archive),
    bucket=Depends(get_s3_bucket),
):
    """
    Get a single alert by candidate id.
    """
    if alert := get_alert_from_s3(candid, db, bucket):
        return alert
    elif alert := db.get_alert(candid, with_history=True):
        return alert
    else:
        raise HTTPException(status.HTTP_404_NOT_FOUND)


@app.get("/alert/{candid}/cutouts", tags=["cutouts"], response_model=AlertCutouts)
def get_cutouts(
    candid: int,
    db: ArchiveDB = Depends(get_archive),
    bucket=Depends(get_s3_bucket),
):
    if alert := get_alert_from_s3(candid, db, bucket):
        return alert
    raise HTTPException(status_code=404)


def verify_authorized_programid(
    programid: Optional[int] = Query(
        None, description="ZTF observing program to query"
    ),
    auth: AuthToken = Depends(verify_access_token),
) -> Optional[int]:
    if not auth.partnership:
        if programid is None:
            return 1
        elif programid != 1:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Not authorized for programid {programid}",
                headers={"WWW-Authenticate": "Bearer"},
            )
    return programid


@app.get(
    "/object/{objectId}/alerts",
    tags=["alerts"],
    response_model=list[Alert],
    response_model_exclude_none=True,
)
def get_alerts_for_object(
    objectId: str = Field(..., description="ZTF object name"),
    jd_start: Optional[float] = Query(
        None, description="minimum Julian Date of observation"
    ),
    jd_end: Optional[float] = Query(
        None, description="maximum Julian Date of observation"
    ),
    with_history: bool = Query(
        False, description="Include previous detections and upper limits"
    ),
    limit: Optional[int] = Query(
        None,
        description="Maximum number of alerts to return",
    ),
    start: int = Query(0, description="Return alerts starting at index"),
    archive: ArchiveDB = Depends(get_archive),
    auth: AuthToken = Depends(verify_access_token),
    programid: Optional[int] = Depends(verify_authorized_programid),
):
    """
    Get all alerts for the given object.
    """
    chunk_id, alerts = archive.get_alerts_for_object(
        objectId,
        jd_start=jd_start,
        jd_end=jd_end,
        programid=programid,
        with_history=with_history,
        limit=limit,
        start=start,
    )
    return alerts


@app.get(
    "/object/{objectId}/photopoints",
    tags=["photopoints"],
    response_model=Alert,  # type: ignore[arg-type]
    response_model_exclude_none=True,
)
def get_photopoints_for_object(
    objectId: str = Field(..., description="ZTF object name"),
    jd_start: Optional[float] = Query(
        None, description="minimum Julian Date of observation"
    ),
    jd_end: Optional[float] = Query(
        None, description="maximum Julian Date of observation"
    ),
    upper_limits: bool = Query(True, description="include upper limits"),
    archive: ArchiveDB = Depends(get_archive),
    auth: AuthToken = Depends(verify_access_token),
    programid: Optional[int] = Depends(verify_authorized_programid),
):
    """
    Get all detections and upper limits for the given object, consolidated into
    a single alert packet.
    """
    return archive.get_photopoints_for_object(
        objectId,
        programid=programid,
        jd_start=jd_start,
        jd_end=jd_end,
        include_upper_limits=upper_limits,
    )


@app.get(
    "/alerts/time_range",
    tags=["search"],
    response_model=AlertChunk,
    response_model_exclude_none=True,
)
def get_alerts_in_time_range(
    jd_start: float = Query(..., description="Earliest observation jd"),
    jd_end: float = Query(..., description="Latest observation jd"),
    with_history: bool = False,
    chunk_size: int = Query(
        100, gt=0, lte=10000, description="Number of alerts to return per page"
    ),
    resume_token: Optional[str] = Query(
        None,
        description="Identifier of a previous query to continue. This token expires after 24 hours.",
    ),
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
    programid: Optional[int] = Depends(verify_authorized_programid),
) -> AlertChunk:
    if resume_token is None:
        resume_token = secrets.token_urlsafe(32)
    chunk, alerts = archive.get_alerts_in_time_range(
        jd_start=jd_start,
        jd_end=jd_end,
        programid=programid,
        with_history=with_history,
        group_name=resume_token,
        block_size=chunk_size,
        max_blocks=1,
    )
    info = get_stream_info(resume_token, archive)
    return AlertChunk(
        resume_token=resume_token,
        alerts=alerts,
        chunk=chunk,
        pending=info["pending"],
        remaining=info["remaining"],
    )


@app.get(
    "/alerts/cone_search",
    tags=["search"],
    response_model=AlertChunk,
    response_model_exclude_none=True,
)
def get_alerts_in_cone(
    ra: float = Query(
        ..., description="Right ascension of field center in degrees (J2000)"
    ),
    dec: float = Query(
        ..., description="Declination of field center in degrees (J2000)"
    ),
    radius: float = Query(..., description="radius of search field in degrees"),
    jd_start: float = Query(..., description="Earliest observation jd"),
    jd_end: float = Query(..., description="Latest observation jd"),
    latest: bool = Query(
        False, description="Return only the latest alert for each objectId"
    ),
    with_history: bool = False,
    chunk_size: int = Query(
        100, gt=0, lte=10000, description="Number of alerts to return per page"
    ),
    resume_token: Optional[str] = Query(
        None,
        description="Identifier of a previous query to continue. This token expires after 24 hours.",
    ),
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
    programid: Optional[int] = Depends(verify_authorized_programid),
) -> AlertChunk:
    if resume_token is None:
        resume_token = secrets.token_urlsafe(32)
    chunk, alerts = archive.get_alerts_in_cone(
        ra=ra,
        dec=dec,
        radius=radius,
        jd_start=jd_start,
        jd_end=jd_end,
        latest=latest,
        programid=programid,
        with_history=with_history,
        group_name=resume_token,
        block_size=chunk_size,
        max_blocks=1,
    )
    info = get_stream_info(resume_token, archive)
    return AlertChunk(
        resume_token=resume_token,
        alerts=alerts,
        chunk=chunk,
        pending=info["pending"],
        remaining=info["remaining"],
    )


@app.get("/alerts/sample")
def get_random_alerts(
    count: int = Query(1, ge=1, le=10_000),
    with_history: bool = Query(False),
    archive: ArchiveDB = Depends(get_archive),
):
    """
    Get a sample of random alerts to test random-access throughput
    """
    alerts, dt = archive.get_random_alerts(count=count, with_history=with_history)
    return {
        "dt": dt,
        "alerts": len(alerts),
        "prv_candidates": sum(len(alert["prv_candidates"]) for alert in alerts),
    }


@app.get(
    "/objects/cone_search",
    tags=["search"],
)
def get_objects_in_cone(
    ra: float = Query(
        ..., description="Right ascension of field center in degrees (J2000)"
    ),
    dec: float = Query(
        ..., description="Declination of field center in degrees (J2000)"
    ),
    radius: float = Query(..., description="radius of search field in degrees"),
    jd_start: float = Query(..., description="Earliest observation jd"),
    jd_end: float = Query(..., description="Latest observation jd"),
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
    programid: Optional[int] = Depends(verify_authorized_programid),
) -> list[str]:
    chunk = list(
        archive.get_objects_in_cone(
            ra=ra,
            dec=dec,
            radius=radius,
            jd_start=jd_start,
            jd_end=jd_end,
            programid=programid,
        )
    )
    return chunk


@app.get(
    "/alerts/healpix",
    tags=["search"],
    response_model=AlertChunk,
    response_model_exclude_none=True,
)
def get_alerts_in_healpix_pixel(
    nside: Literal[
        "1",
        "2",
        "4",
        "8",
        "16",
        "32",
        "64",
        "128",
        "256",
        "512",
        "1024",
        "2048",
        "4096",
        "8192",
    ] = Query("64", description="NSide of (nested) HEALpix grid"),
    ipix: list[int] = Query(..., description="Pixel index"),
    jd_start: float = Query(..., description="Earliest observation jd"),
    jd_end: float = Query(..., description="Latest observation jd"),
    latest: bool = Query(
        False, description="Return only the latest alert for each objectId"
    ),
    with_history: bool = False,
    chunk_size: int = Query(
        100, gt=0, lte=10000, description="Number of alerts to return per page"
    ),
    resume_token: Optional[str] = Query(
        None,
        description="Identifier of a previous query to continue. This token expires after 24 hours.",
    ),
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
    programid: Optional[int] = Depends(verify_authorized_programid),
) -> AlertChunk:
    if resume_token is None:
        resume_token = secrets.token_urlsafe(32)
    chunk, alerts = archive.get_alerts_in_healpix(
        pixels={int(nside): ipix},
        jd_start=jd_start,
        jd_end=jd_end,
        latest=latest,
        programid=programid,
        with_history=with_history,
        group_name=resume_token,
        block_size=chunk_size,
        max_blocks=1,
    )
    info = get_stream_info(resume_token, archive)
    return AlertChunk(
        resume_token=resume_token,
        alerts=alerts,
        chunk=chunk,
        pending=info["pending"],
        remaining=info["remaining"],
    )


@app.post(
    "/alerts/healpix/skymap",
    tags=["search"],
    response_model=AlertChunk,
    response_model_exclude_none=True,
)
def get_alerts_in_healpix_map(
    query: Union[HEALpixMapQuery, HEALpixRegionQuery],
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
    programid: Optional[int] = Depends(verify_authorized_programid),
) -> AlertChunk:
    resume_token = query.resume_token or secrets.token_urlsafe(32)
    if query.resume_token:
        chunk, alerts = archive.get_chunk_from_queue(
            query.resume_token, with_history=query.with_history
        )
    else:
        if isinstance(query, HEALpixRegionQuery):
            regions = {region.nside: region.pixels for region in query.regions}
        else:
            regions = deres(query.nside, query.pixels)
        chunk, alerts = archive.get_alerts_in_healpix(
            pixels=regions,
            jd_start=query.jd.gt,
            jd_end=query.jd.lt,
            latest=query.latest,
            programid=programid,
            candidate_filter=query.candidate,
            with_history=query.with_history,
            group_name=resume_token,
            block_size=query.chunk_size,
            max_blocks=1,
        )
    info = get_stream_info(resume_token, archive)
    return AlertChunk(
        resume_token=resume_token,
        alerts=alerts,
        chunk=chunk,
        pending=info["pending"],
        remaining=info["remaining"],
    )


@app.post(
    "/alerts/healpix/skymap/count",
    tags=["search"],
    response_model=AlertCount,
    response_model_exclude_none=True,
)
def count_alerts_in_healpix_map(
    query: HEALpixRegionCountQuery,
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
    programid: Optional[int] = Depends(verify_authorized_programid),
) -> AlertCount:
    return AlertCount(
        count=archive.count_alerts_in_healpix(
            pixels={region.nside: region.pixels for region in query.regions},
            jd_start=query.jd.gt,
            jd_end=query.jd.lt,
            programid=programid,
            candidate_filter=query.candidate,
        )
    )


@app.post("/topics/", tags=["topic"], status_code=201)
def create_topic(
    topic: Topic,
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
):
    """
    Create a new persistent collection of alerts
    """
    name = secrets.token_urlsafe()
    try:
        archive.create_topic(name, topic.candids, topic.description)
    except sqlalchemy.exc.IntegrityError:
        raise HTTPException(
            status_code=400,
            detail={
                "msg": "Topic did not match any alerts. Are you sure these are valid candidate ids?",
                "topic": jsonable_encoder(topic),
            },
        )
    return name


@app.get("/topic/{topic}", tags=["topic"], response_model=TopicDescription)
def get_topic(
    topic: str,
    archive: ArchiveDB = Depends(get_archive),
):
    return {"topic": topic, **archive.get_topic_info(topic)}


@app.post(
    "/streams/from_topic",
    tags=["topic", "stream"],
    response_model=StreamDescription,
    status_code=201,
)
def create_stream_from_topic(
    query: TopicQuery,
    archive: ArchiveDB = Depends(get_archive),
):
    """
    Create a stream of alerts from the given persistent topic.  The resulting
    resume_token can be used to read the stream concurrently from multiple clients.
    """
    resume_token = secrets.token_urlsafe()
    try:
        archive.create_read_queue_from_topic(
            query.topic,
            resume_token,
            query.chunk_size,
            slice(query.start, query.stop, query.step),
        )
    except GroupNotFoundError:
        raise HTTPException(status_code=404, detail="Topic not found")
    stream_info = get_stream_info(resume_token, archive)
    return {"resume_token": resume_token, **stream_info}


@app.post(
    "/streams/from_query",
    tags=["search", "stream"],
    response_model=Stream,
    status_code=status.HTTP_202_ACCEPTED,
)
def create_stream_from_query(
    tasks: BackgroundTasks,
    query: Union[AlertQuery, ObjectQuery, HEALpixRegionQuery] = Body(
        ...,
        examples={
            "cone": {
                "summary": "Cone search",
                "value": {
                    "cone": {"ra": 158.068431, "dec": 47.0497302, "radius": 3.5},
                },
            },
            "object": {
                "summary": "ZTF-ID search",
                "description": "Retrieve alerts for all ObjectIds provided",
                "value": {
                    "objectId": ["ZTF19aapreis", "ZTF19aatubsj"],
                    "jd": {"$gt": 2458550.5, "$lt": 2459550.5},
                },
            },
            "healpix": {
                "summary": "HEALpix search",
                "description": "search regions of a HEALpix map (in narrow time range)",
                "value": {
                    "regions": [{"nside": 64, "pixels": [5924, 5925, 5926, 5927]}],
                    "jd": {"$gt": 2459308.72, "$lt": 2459308.73},
                },
            },
            "filtered": {
                "summary": "Epoch search",
                "description": "Search for all candidates in an epoch range that fulfill criteria",
                "value": {
                    "jd": {"$gt": 2459550.5, "$lt": 2459551.5},
                    "candidate": {
                        "drb": {"$gt": 0.999},
                        "magpsf": {"$lt": 15},
                        "ndethist": {"$gt": 0, "$lte": 10},
                        "fid": 1,
                    },
                },
            },
        },
    ),
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
    programid: Optional[int] = Depends(verify_authorized_programid),
):
    """
    Create a stream of alerts from the given query. The resulting resume_token
    can be used to read the stream concurrently from multiple clients.
    """
    try:
        if isinstance(query, AlertQuery):
            if query.cone:
                condition, order = archive._cone_search_condition(
                    ra=query.cone.ra,
                    dec=query.cone.dec,
                    radius=query.cone.radius,
                    programid=programid,
                    jd_min=query.jd.gt,
                    jd_max=query.jd.lt,
                    candidate_filter=query.candidate,
                )
            else:
                condition, order = archive._time_range_condition(
                    programid=programid,
                    jd_start=query.jd.gt,
                    jd_end=query.jd.lt,
                    candidate_filter=query.candidate,
                )
        elif isinstance(query, ObjectQuery):
            condition, order = archive._object_search_condition(
                objectId=query.objectId,
                programid=programid,
                jd_start=query.jd.gt,
                jd_end=query.jd.lt,
                candidate_filter=query.candidate,
            )
        else:
            pixels: dict[int, list[int]] = {}
            for region in query.regions:
                pixels[region.nside] = pixels.get(region.nside, []) + region.pixels
            condition, order = archive._healpix_search_condition(
                pixels=pixels,
                jd_min=query.jd.gt,
                jd_max=query.jd.lt,
                latest=query.latest,
                programid=programid,
                candidate_filter=query.candidate,
            )
    except NoSuchColumnError as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"msg": f"unknown candidate field {exc.args[0]}"},
        )

    name = secrets.token_urlsafe(32)
    try:
        conn = archive._engine.connect()
    except sqlalchemy.exc.TimeoutError as exc:
        conn.close()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail={"msg": str(exc)}
        )

    conn.execute(f"set statement_timeout={settings.stream_query_timeout*1000};")
    group_id = archive._create_read_queue(conn, name, query.chunk_size)

    # create stream in the background
    def create_stream() -> None:
        try:
            archive._fill_read_queue(conn, condition, order, group_id, query.chunk_size)
        finally:
            conn.close()

    tasks.add_task(create_stream)

    return {"resume_token": name, "chunk_size": query.chunk_size}


def get_stream_info(resume_token: str, archive: ArchiveDB = Depends(get_archive)):
    try:
        info = archive.get_group_info(resume_token)
    except GroupNotFoundError:
        raise HTTPException(status_code=404, detail="Stream not found")
    if info["error"] is None:
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail={"msg": "queue-populating query has not yet finished"},
        )
    elif info["error"]:
        raise HTTPException(
            status_code=status.HTTP_424_FAILED_DEPENDENCY,
            detail={"msg": info["msg"]},
        )
    return info


@app.get(
    "/stream/{resume_token}",
    tags=["stream"],
    response_model=StreamDescription,
    response_model_exclude_none=True,
    responses={
        status.HTTP_423_LOCKED: {"description": "Query has not finished"},
        status.HTTP_424_FAILED_DEPENDENCY: {"description": "Query failed"},
    },
)
def get_stream(resume_token: str, stream_info: GroupInfo = Depends(get_stream_info)):
    """
    Get the next available chunk of alerts from the given stream.
    """
    return {"resume_token": resume_token, **stream_info}


@app.get(
    "/stream/{resume_token}/chunk",
    tags=["stream"],
    response_model=AlertChunk,
    response_model_exclude_none=True,
    responses={
        status.HTTP_423_LOCKED: {"description": "Query has not finished"},
        status.HTTP_424_FAILED_DEPENDENCY: {"description": "Query failed"},
    },
)
def stream_get_chunk(
    resume_token: str,
    with_history: bool = True,
    archive: ArchiveDB = Depends(get_archive),
    # piggy-back on stream info to raise errors on pending or errored queries
    stream_info=Depends(get_stream_info),
):
    """
    Get the next available chunk of alerts from the given stream. This chunk will
    be reserved until explicitly acknowledged.
    """
    try:
        chunk_id, alerts = archive.get_chunk_from_queue(resume_token, with_history)
    except GroupNotFoundError:
        raise HTTPException(status_code=404, detail="Stream not found")
    info = get_stream_info(resume_token, archive)
    return AlertChunk(
        resume_token=resume_token,
        chunk=chunk_id,
        alerts=alerts,
        remaining=info["remaining"],
        pending=info["pending"],
    )


@app.post(
    "/stream/{resume_token}/chunk/{chunk_id}/acknowledge",
    tags=["stream"],
)
def stream_acknowledge_chunk(
    resume_token: str,
    chunk_id: int,
    archive: ArchiveDB = Depends(get_archive),
    # piggy-back on stream info to raise errors on pending or errored queries
    stream_info=Depends(get_stream_info),
):
    """
    Mark the given chunk as consumed.
    """
    archive.acknowledge_chunk_from_queue(resume_token, chunk_id)


@app.post(
    "/stream/{resume_token}/chunk/{chunk_id}/release",
    tags=["stream"],
)
def stream_release_chunk(
    resume_token: str,
    chunk_id: int,
    archive: ArchiveDB = Depends(get_archive),
    # piggy-back on stream info to raise errors on pending or errored queries
    stream_info=Depends(get_stream_info),
):
    """
    Mark the given chunk as unconsumed.
    """
    archive.release_chunk_from_queue(resume_token, chunk_id)


# If we are mounted under a (non-stripped) prefix path, create a potemkin root
# router and mount the actual root as a sub-application. This has no effect
# other than to prefix the paths of all routes with the root path.
if settings.root_path:
    wrapper = FastAPI()
    wrapper.mount(settings.root_path, app)
    app = wrapper
