from ampel.ztf.archive.server.models import AlertChunk
import secrets
from base64 import b64encode
from functools import lru_cache
from typing import Optional

from fastapi import FastAPI, Depends, Query, HTTPException, status
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from .settings import Settings
from .models import (
    AlertChunk,
    AlertQuery,
    StreamDescription,
    Topic,
    TopicDescription,
    TopicQuery,
)
from ampel.ztf.archive.ArchiveDB import ArchiveDB, GroupNotFoundError

settings = Settings()

app = FastAPI(
    title="ZTF Alert Archive Service",
    description="Query ZTF alerts issued by IPAC",
    version="1.0.0",
    root_path=settings.root_path,
)

app.add_middleware(GZipMiddleware, minimum_size=1000)


@lru_cache(maxsize=1)
def get_archive():
    return ArchiveDB(settings.archive_uri)


security = HTTPBasic()


def authorized(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(
        credentials.username,
        settings.auth_user,
    )
    correct_password = secrets.compare_digest(
        credentials.password,
        settings.auth_password,
    )
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True


@app.get("/alert/{candid}")
def get_alert(
    candid: int,
    with_history: bool = True,
    with_cutouts: bool = False,
    archive: ArchiveDB = Depends(get_archive),
):
    return archive.get_alert(candid, with_history, with_cutouts)


@app.get("/cutouts/{candid}")
def get_cutouts(
    candid: int,
    archive: ArchiveDB = Depends(get_archive),
):
    if (cutouts := archive.get_cutout(candid)):
        return {k: b64encode(v) for k,v in cutouts.items()}
    else:
        raise HTTPException(status_code=404)


@app.get("/object/{objectId}/alerts")
def get_alerts_for_object(
    objectId: str,
    jd_start: Optional[float] = None,
    jd_end: Optional[float] = None,
    with_history: bool = False,
    with_cutouts: bool = False,
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(authorized),
):
    return archive.get_alerts_for_object(
        objectId, jd_start, jd_end, with_history, with_cutouts
    )


@app.get("/object/{objectId}/photopoints")
def get_photopoints_for_object(
    objectId: str,
    programid: Optional[int] = None,
    jd_start: Optional[float] = None,
    jd_end: Optional[float] = None,
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(authorized),
):
    return archive.get_photopoints_for_object(objectId, programid, jd_start, jd_end)


@app.get("/alerts/time_range", response_model=AlertChunk)
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
    auth: bool = Depends(authorized),
) -> AlertChunk:
    if resume_token is None:
        resume_token = secrets.token_urlsafe(32)
    chunk = list(
        archive.get_alerts_in_time_range(
            jd_min=jd_start,
            jd_max=jd_end,
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


@app.get("/alerts/cone_search", response_model=AlertChunk)
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
    auth: bool = Depends(authorized),
) -> AlertChunk:
    if resume_token is None:
        resume_token = secrets.token_urlsafe(32)
    chunk = list(
        archive.get_alerts_in_cone(
            ra=ra,
            dec=dec,
            radius=radius,
            jd_min=jd_start,
            jd_max=jd_end,
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


@app.post("/topics/", status_code=201)
def create_topic(
    topic: Topic,
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(authorized),
):
    """
    Create a new persistent collection of alerts
    """
    name = secrets.token_urlsafe()
    archive.create_topic(name, topic.candids, topic.description)
    return name


@app.get("/topic/{topic}", response_model=TopicDescription)
def get_topic(
    topic: str,
    archive: ArchiveDB = Depends(get_archive),
):
    return {"topic": topic, **archive.get_topic_info(topic)}


@app.post("/streams/from_topic", response_model=StreamDescription, status_code=201)
def create_stream_from_topic(
    query: TopicQuery,
    archive: ArchiveDB = Depends(get_archive),
):
    """
    Create a stream of alerts from the given persistent topic
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


@app.post("/streams/from_query", response_model=StreamDescription, status_code=201)
def create_stream_from_query(
    query: AlertQuery,
    archive: ArchiveDB = Depends(get_archive),
    auth: bool = Depends(authorized),
):
    """
    Create a stream of alerts from the given query. The resulting resume_token
    can be used to read the stream concurrently from multiple clients.
    """
    if query.cone:
        condition, order = archive._cone_search_condition(
            query.cone.ra,
            query.cone.dec,
            query.cone.radius,
            query.programid,
            query.jd.gt,
            query.jd.lt,
        )
    else:
        condition, order = archive._time_range_condition(
            query.programid,
            query.jd.gt,
            query.jd.lt,
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


@app.get("/stream/{resume_token}/chunk", response_model=AlertChunk)
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
