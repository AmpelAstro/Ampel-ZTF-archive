from ampel.ztf.archive.server.models import AlertChunk
import secrets
from functools import lru_cache
from typing import Optional

from fastapi import FastAPI, Depends, Query, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from .settings import settings
from .models import AlertChunk
from ampel.ztf.archive.ArchiveDB import ArchiveDB

app = FastAPI(
    title="ZTF Alert Archive Service",
    description="Query ZTF alerts issued by IPAC",
    version="1.0.0",
    root_path=settings.root_path,
)


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


@app.get("/alerts/time_range")
def get_alerts_in_time_range(
    jd_start: float = Query(..., description="Earliest observation jd"),
    jd_end: float = Query(..., description="Latest observation jd"),
    programid: Optional[int] = None,
    with_history: bool = False,
    with_cutouts: bool = False,
    chunk_size: int = Query(
        100,  gt=0, lte=10000, description="Number of alerts to return per page"
    ),
    resume_token: Optional[str] = Query(
        None, description="Identifier of a previous query to continue"
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


@app.get("/alerts/cone_search")
def get_alerts_in_cone(
    ra: float = Query(..., description="Right ascension of field center in degrees (J2000)"),
    dec: float = Query(..., description="Declination of field center in degrees (J2000)"),
    radius: float = Query(..., description="radius of search field in degrees"),
    jd_start: float = Query(..., description="Earliest observation jd"),
    jd_end: float = Query(..., description="Latest observation jd"),
    programid: Optional[int] = None,
    with_history: bool = False,
    with_cutouts: bool = False,
    chunk_size: int = Query(
        100,  gt=0, lte=10000, description="Number of alerts to return per page"
    ),
    resume_token: Optional[str] = Query(
        None, description="Identifier of a previous query to continue"
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

# If we are mounted under a (non-stripped) prefix path, create a potemkin root
# router and mount the actual root as a sub-application. This has no effect
# other than to prefix the paths of all routes with the root path.
if settings.root_path:
    wrapper = FastAPI()
    wrapper.mount(settings.root_path, app)
    app = wrapper
