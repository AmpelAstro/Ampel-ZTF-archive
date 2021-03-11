import secrets
from functools import lru_cache
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from .settings import settings
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


# If we are mounted under a (non-stripped) prefix path, create a potemkin root
# router and mount the actual root as a sub-application. This has no effect
# other than to prefix the paths of all routes with the root path.
if settings.root_path:
    wrapper = FastAPI()
    wrapper.mount(settings.root_path, app)
    app = wrapper
