from base64 import b64encode
from chunk import Chunk
from typing import List, Dict, Any, Literal, Optional, Union
from pydantic import BaseModel, Field, validator, root_validator
from ..types import FilterClause


class StrictModel(BaseModel):
    class Config:
        extra = "forbid"


class Stream(BaseModel):
    resume_token: str
    chunk_size: int


class ChunkCount(BaseModel):
    items: int
    chunks: int


class StreamDescription(Stream):
    remaining: ChunkCount
    pending: ChunkCount


class Topic(BaseModel):
    description: str = Field(..., description="Informative string for this topic")
    candids: List[int] = Field(
        ..., description="IPAC candidate ids to associate with this topic"
    )


class TopicDescription(BaseModel):
    topic: str
    description: str = Field(..., description="Informative string for this topic")
    size: int


class TopicQuery(StrictModel):
    topic: str
    chunk_size: int = Field(
        100, gte=100, lte=10000, description="Number of alerts per chunk"
    )
    start: Optional[int] = Field(None, gte=0)
    stop: Optional[int] = Field(None, gte=1)
    step: Optional[int] = Field(None, gt=0)


class ConeConstraint(StrictModel):
    ra: float = Field(
        ..., description="Right ascension of field center in degrees (J2000)"
    )
    dec: float = Field(
        ..., description="Declination of field center in degrees (J2000)"
    )
    radius: float = Field(
        ..., gt=0, lt=10, description="Radius of search cone in degrees"
    )


class TimeConstraint(StrictModel):
    lt: Optional[float] = Field(None, alias="$lt")
    gt: Optional[float] = Field(None, alias="$gt")


class StrictTimeConstraint(TimeConstraint):
    lt: float = Field(..., alias="$lt")
    gt: float = Field(..., alias="$gt")


class CandidateFilterable(StrictModel):
    candidate: Optional[FilterClause] = None

    @validator("candidate", pre=True, each_item=True)
    def validate_operator(cls, v):
        if isinstance(v, dict):
            return v
        else:
            return {"$eq": v}


class AlertQuery(CandidateFilterable):
    cone: Optional[ConeConstraint] = None
    jd: TimeConstraint = TimeConstraint()
    candidate: Optional[FilterClause] = None
    chunk_size: int = Field(
        100, gte=0, lte=10000, description="Number of alerts per chunk"
    )

    @root_validator
    def at_least_one_constraint(cls, values):
        if not {"cone", "jd"}.intersection(values.keys()):
            raise ValueError(f"At least one constraint (cone or jd) must be specified")
        return values


class ObjectQuery(CandidateFilterable):
    objectId: Union[str, List[str]]
    jd: TimeConstraint = TimeConstraint()
    candidate: Optional[FilterClause] = None
    chunk_size: int = Field(
        100, gte=0, lte=10000, description="Number of alerts per chunk"
    )


class MapQueryBase(CandidateFilterable):
    jd: StrictTimeConstraint
    latest: bool = Field(
        False, description="Return only the latest alert for each objectId"
    )
    with_history: bool = False
    with_cutouts: bool = False
    chunk_size: int = Field(
        100, gt=0, lte=10000, description="Number of alerts to return per page"
    )
    resume_token: Optional[str] = Field(
        None,
        description="Identifier of a previous query to continue. This token expires after 24 hours.",
    )


class HEALpixMapRegion(StrictModel):
    nside: int
    pixels: list[int]


class HEALpixMapQuery(MapQueryBase, HEALpixMapRegion):
    ...


class HEALpixRegionQuery(MapQueryBase):
    regions: list[HEALpixMapRegion]


# Generated from tests/test-data/schema_3.3.json
# 1. Convert avro to json-schema with https://json-schema-validator.herokuapp.com/avro.jsp
# 2. Convert json-schema to pydantic with datamodel-codegen --input schema_3.3.json --output alert --use-schema-description


class Candidate(BaseModel):
    """
    avro alert schema
    """

    jd: float
    fid: int
    pid: int
    diffmaglim: Optional[float]
    pdiffimfilename: Optional[str]
    programpi: Optional[str]
    programid: Literal[1, 2, 3]
    candid: int
    isdiffpos: str
    tblid: int
    nid: Optional[int]
    rcid: Optional[int]
    field: Optional[int]
    xpos: Optional[float]
    ypos: Optional[float]
    ra: float
    dec: float
    magpsf: float
    sigmapsf: float
    chipsf: Optional[float]
    magap: Optional[float]
    sigmagap: Optional[float]
    distnr: Optional[float]
    magnr: Optional[float]
    sigmagnr: Optional[float]
    chinr: Optional[float]
    sharpnr: Optional[float]
    sky: Optional[float]
    magdiff: Optional[float]
    fwhm: Optional[float]
    classtar: Optional[float]
    mindtoedge: Optional[float]
    magfromlim: Optional[float]
    seeratio: Optional[float]
    aimage: Optional[float]
    bimage: Optional[float]
    aimagerat: Optional[float]
    bimagerat: Optional[float]
    elong: Optional[float]
    nneg: Optional[int]
    nbad: Optional[int]
    rb: Optional[float]
    ssdistnr: Optional[float]
    ssmagnr: Optional[float]
    ssnamenr: Optional[str]
    sumrat: Optional[float]
    magapbig: Optional[float]
    sigmagapbig: Optional[float]
    ranr: float
    decnr: float
    sgmag1: Optional[float]
    srmag1: Optional[float]
    simag1: Optional[float]
    szmag1: Optional[float]
    sgscore1: Optional[float]
    distpsnr1: Optional[float]
    ndethist: int
    ncovhist: int
    jdstarthist: Optional[float]
    jdendhist: Optional[float]
    scorr: Optional[float]
    tooflag: Optional[int]
    objectidps1: Optional[int]
    objectidps2: Optional[int]
    sgmag2: Optional[float]
    srmag2: Optional[float]
    simag2: Optional[float]
    szmag2: Optional[float]
    sgscore2: Optional[float]
    distpsnr2: Optional[float]
    objectidps3: Optional[int]
    sgmag3: Optional[float]
    srmag3: Optional[float]
    simag3: Optional[float]
    szmag3: Optional[float]
    sgscore3: Optional[float]
    distpsnr3: Optional[float]
    nmtchps: int
    rfid: int
    jdstartref: float
    jdendref: float
    nframesref: int
    rbversion: Optional[str]
    dsnrms: Optional[float]
    ssnrms: Optional[float]
    dsdiff: Optional[float]
    magzpsci: Optional[float]
    magzpsciunc: Optional[float]
    magzpscirms: Optional[float]
    nmatches: Optional[int]
    clrcoeff: Optional[float]
    clrcounc: Optional[float]
    zpclrcov: Optional[float]
    zpmed: Optional[float]
    clrmed: Optional[float]
    clrrms: Optional[float]
    neargaia: Optional[float]
    neargaiabright: Optional[float]
    maggaia: Optional[float]
    maggaiabright: Optional[float]
    exptime: Optional[float]
    drb: Optional[float]
    drbversion: Optional[str]


class PrvCandidate(BaseModel):
    """
    avro alert schema
    """

    jd: float
    fid: int
    pid: int
    diffmaglim: Optional[float]
    pdiffimfilename: Optional[str]
    programpi: Optional[str]
    programid: int
    candid: Optional[int]
    isdiffpos: Optional[str]
    tblid: Optional[int]
    nid: Optional[int]
    rcid: Optional[int]
    field: Optional[int]
    xpos: Optional[float]
    ypos: Optional[float]
    ra: Optional[float]
    dec: Optional[float]
    magpsf: Optional[float]
    sigmapsf: Optional[float]
    chipsf: Optional[float]
    magap: Optional[float]
    sigmagap: Optional[float]
    distnr: Optional[float]
    magnr: Optional[float]
    sigmagnr: Optional[float]
    chinr: Optional[float]
    sharpnr: Optional[float]
    sky: Optional[float]
    magdiff: Optional[float]
    fwhm: Optional[float]
    classtar: Optional[float]
    mindtoedge: Optional[float]
    magfromlim: Optional[float]
    seeratio: Optional[float]
    aimage: Optional[float]
    bimage: Optional[float]
    aimagerat: Optional[float]
    bimagerat: Optional[float]
    elong: Optional[float]
    nneg: Optional[int]
    nbad: Optional[int]
    rb: Optional[float]
    ssdistnr: Optional[float]
    ssmagnr: Optional[float]
    ssnamenr: Optional[str]
    sumrat: Optional[float]
    magapbig: Optional[float]
    sigmagapbig: Optional[float]
    ranr: Optional[float]
    decnr: Optional[float]
    scorr: Optional[float]
    magzpsci: Optional[float]
    magzpsciunc: Optional[float]
    magzpscirms: Optional[float]
    clrcoeff: Optional[float]
    clrcounc: Optional[float]
    rbversion: Optional[str]


class Cutout(BaseModel):
    """
    stampData is a gzipped FITS file, b64 encoded
    """

    fileName: str
    stampData: bytes


class AlertBase(BaseModel):
    candid: int
    objectId: str

    class Config:
        json_encoders = {bytes: lambda v: b64encode(v).decode()}


class AlertCutouts(AlertBase):
    cutoutScience: Optional[Cutout]
    cutoutTemplate: Optional[Cutout]
    cutoutDifference: Optional[Cutout]


class Alert(AlertCutouts):
    """
    avro alert schema for ZTF (www.ztf.caltech.edu)
    """

    schemavsn: str = "3.3"
    publisher: str = "Ampel"
    candidate: Candidate
    prv_candidates: Optional[List[PrvCandidate]]


class AlertChunk(BaseModel):
    resume_token: str
    chunk: Optional[int]
    alerts: List[Alert]
    remaining: ChunkCount
    pending: ChunkCount

    class Config:
        json_encoders = {bytes: lambda v: b64encode(v).decode()}
