from ampel.ztf.archive.server.skymap import deres
from pydantic.fields import Field
import sqlalchemy
from ampel.ztf.archive.server.models import AlertChunk
import secrets
from base64 import b64encode
from typing import List, Literal, Optional, Union

from fastapi import FastAPI, Depends, Query, HTTPException, status
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from .settings import settings
from .db import get_archive
from .tokens import router as token_router, verify_access_token
from .models import (
    Alert,
    AlertChunk,
    AlertCutouts,
    AlertQuery,
    HEALpixMapQuery,
    StreamDescription,
    Topic,
    TopicDescription,
    TopicQuery,
)
from ampel.ztf.archive.ArchiveDB import ArchiveDB, GroupNotFoundError

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
    version="2.1.0",
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


@app.get(
    "/alert/{candid}",
    tags=["alerts"],
    response_model=Alert,
    response_model_exclude_none=True,
)
def get_alert(
    candid: int,
    with_history: bool = True,
    with_cutouts: bool = False,
    archive: ArchiveDB = Depends(get_archive),
):
    """
    Get a single alert by candidate id.
    """
    return archive.get_alert(
        candid, with_history=with_history, with_cutouts=with_cutouts
    )


@app.get("/cutouts/{candid}", tags=["cutouts"], response_model=AlertCutouts)
def get_cutouts(
    candid: int,
    archive: ArchiveDB = Depends(get_archive),
):
    if cutouts := archive.get_cutout(candid):
        return {k: b64encode(v) for k, v in cutouts.items()}
    else:
        raise HTTPException(status_code=404)


@app.get(
    "/object/{objectId}/alerts",
    tags=["alerts"],
    response_model=List[Alert],
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
    with_cutouts: bool = Query(
        False, description="Include image cutouts (if available)"
    ),
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
):
    """
    Get all alerts for the given object.
    """
    return archive.get_alerts_for_object(
        objectId,
        jd_start=jd_start,
        jd_end=jd_end,
        with_history=with_history,
        with_cutouts=with_cutouts,
    )


@app.get(
    "/object/{objectId}/photopoints",
    tags=["photopoints"],
    response_model=Alert,
    response_model_exclude_none=True,
)
def get_photopoints_for_object(
    objectId: str = Field(..., description="ZTF object name"),
    programid: Optional[Literal[1, 2, 3]] = Query(
        None, description="ZTF observing program for which to return photopoints"
    ),
    jd_start: Optional[float] = Query(
        None, description="minimum Julian Date of observation"
    ),
    jd_end: Optional[float] = Query(
        None, description="maximum Julian Date of observation"
    ),
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
):
    """
    Get all detections and upper limits for the given object, consolidated into
    a single alert packet.
    """
    return archive.get_photopoints_for_object(
        objectId, programid=programid, jd_start=jd_start, jd_end=jd_end
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
    programid: Optional[int] = None,
    with_history: bool = False,
    with_cutouts: bool = False,
    chunk_size: int = Query(
        100, gt=0, lte=10000, description="Number of alerts to return per page"
    ),
    resume_token: Optional[str] = Query(
        None,
        description="Identifier of a previous query to continue. This token expires after 24 hours.",
    ),
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
) -> AlertChunk:
    if resume_token is None:
        resume_token = secrets.token_urlsafe(32)
    chunk = list(
        archive.get_alerts_in_time_range(
            jd_start=jd_start,
            jd_end=jd_end,
            programid=programid,
            with_history=with_history,
            with_cutouts=with_cutouts,
            group_name=resume_token,
            block_size=chunk_size,
            max_blocks=1,
        )
    )
    return AlertChunk(
        resume_token=resume_token,
        chunk_size=chunk_size,
        chunks_remaining=archive.get_remaining_chunks(resume_token),
        alerts=chunk,
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
    programid: Optional[int] = None,
    latest: bool = Query(
        False, description="Return only the latest alert for each objectId"
    ),
    with_history: bool = False,
    with_cutouts: bool = False,
    chunk_size: int = Query(
        100, gt=0, lte=10000, description="Number of alerts to return per page"
    ),
    resume_token: Optional[str] = Query(
        None,
        description="Identifier of a previous query to continue. This token expires after 24 hours.",
    ),
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
) -> AlertChunk:
    if resume_token is None:
        resume_token = secrets.token_urlsafe(32)
    chunk = list(
        archive.get_alerts_in_cone(
            ra=ra,
            dec=dec,
            radius=radius,
            jd_start=jd_start,
            jd_end=jd_end,
            latest=latest,
            programid=programid,
            with_history=with_history,
            with_cutouts=with_cutouts,
            group_name=resume_token,
            block_size=chunk_size,
            max_blocks=1,
        )
    )
    return AlertChunk(
        resume_token=resume_token,
        chunk_size=chunk_size,
        chunks_remaining=archive.get_remaining_chunks(resume_token),
        alerts=chunk,
    )


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
    programid: Optional[int] = None,
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
) -> List[str]:
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
    ipix: List[int] = Query(..., description="Pixel index"),
    jd_start: float = Query(..., description="Earliest observation jd"),
    jd_end: float = Query(..., description="Latest observation jd"),
    latest: bool = Query(
        False, description="Return only the latest alert for each objectId"
    ),
    with_history: bool = False,
    with_cutouts: bool = False,
    chunk_size: int = Query(
        100, gt=0, lte=10000, description="Number of alerts to return per page"
    ),
    resume_token: Optional[str] = Query(
        None,
        description="Identifier of a previous query to continue. This token expires after 24 hours.",
    ),
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
) -> AlertChunk:
    if resume_token is None:
        resume_token = secrets.token_urlsafe(32)
    chunk = list(
        archive.get_alerts_in_healpix(
            pixels={int(nside): ipix},
            jd_start=jd_start,
            jd_end=jd_end,
            latest=latest,
            with_history=with_history,
            with_cutouts=with_cutouts,
            group_name=resume_token,
            block_size=chunk_size,
            max_blocks=1,
        )
    )
    return AlertChunk(
        resume_token=resume_token,
        chunk_size=chunk_size,
        chunks_remaining=archive.get_remaining_chunks(resume_token),
        alerts=chunk,
    )


@app.post(
    "/alerts/healpix/skymap",
    tags=["search"],
    response_model=AlertChunk,
    response_model_exclude_none=True,
)
def get_alerts_in_healpix_map(
    query: HEALpixMapQuery,
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
) -> AlertChunk:
    resume_token = query.resume_token or secrets.token_urlsafe(32)
    chunk = list(
        archive.get_alerts_in_healpix(
            pixels=deres(query.nside, query.pixels),
            jd_start=query.jd.gt,
            jd_end=query.jd.lt,
            latest=query.latest,
            with_history=query.with_history,
            with_cutouts=query.with_cutouts,
            group_name=resume_token,
            block_size=query.chunk_size,
            max_blocks=1,
        )
    )
    return AlertChunk(
        resume_token=resume_token,
        chunk_size=query.chunk_size,
        chunks_remaining=archive.get_remaining_chunks(resume_token),
        alerts=chunk,
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
    name = secrets.token_urlsafe()
    try:
        queue_info = archive.create_read_queue_from_topic(
            query.topic,
            name,
            query.chunk_size,
            slice(query.start, query.stop, query.step),
        )
    except GroupNotFoundError:
        raise HTTPException(status_code=404, detail="Topic not found")
    return {
        "resume_token": name,
        "chunk_size": query.chunk_size,
        "chunks": queue_info["chunks"],
    }


@app.post(
    "/streams/from_query",
    tags=["search", "stream"],
    response_model=StreamDescription,
    status_code=201,
)
def create_stream_from_query(
    query: Union[AlertQuery, HEALpixMapQuery],
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(verify_access_token),
):
    """
    Create a stream of alerts from the given query. The resulting resume_token
    can be used to read the stream concurrently from multiple clients.
    """
    if isinstance(query, AlertQuery):
        if query.cone:
            condition, order = archive._cone_search_condition(
                ra=query.cone.ra,
                dec=query.cone.dec,
                radius=query.cone.radius,
                programid=query.programid,
                jd_min=query.jd.gt,
                jd_max=query.jd.lt,
            )
        else:
            condition, order = archive._time_range_condition(
                query.programid,
                query.jd.gt,
                query.jd.lt,
            )
    else:
        condition, order = archive._healpix_search_condition(
            pixels=deres(query.nside, query.pixels),
            jd_min=query.jd.gt,
            jd_max=query.jd.lt,
            latest=query.latest,
        )

    with archive._engine.connect() as conn:
        name = secrets.token_urlsafe(32)
        group_id, chunks = archive._create_read_queue(
            conn, condition, order, name, query.chunk_size
        )

    return {
        "resume_token": name,
        "chunk_size": query.chunk_size,
        "chunks": chunks,
    }


@app.get(
    "/stream/{resume_token}/chunk",
    tags=["stream"],
    response_model=AlertChunk,
    response_model_exclude_none=True,
)
def stream_get_chunk(
    resume_token: str,
    with_history: bool = True,
    with_cutouts: bool = False,
    archive: ArchiveDB = Depends(get_archive),
):
    """
    Get the next available chunk of alerts from the given stream.
    """
    try:
        chunk = list(
            archive.get_chunk_from_queue(resume_token, with_history, with_cutouts)
        )
    except GroupNotFoundError:
        raise HTTPException(status_code=404, detail="Stream not found")
    return AlertChunk(
        resume_token=resume_token,
        chunks_remaining=archive.get_remaining_chunks(resume_token),
        alerts=chunk,
    )


# If we are mounted under a (non-stripped) prefix path, create a potemkin root
# router and mount the actual root as a sub-application. This has no effect
# other than to prefix the paths of all routes with the root path.
if settings.root_path:
    wrapper = FastAPI()
    wrapper.mount(settings.root_path, app)
    app = wrapper
