from typing import List, Dict, Any, Literal, Optional
from pydantic import BaseModel, Field


class StreamDescription(BaseModel):
    resume_token: str
    chunk_size: int
    chunks: int


class Topic(BaseModel):
    description: str = Field(..., description="Informative string for this topic")
    candids: List[int] = Field(
        ..., description="IPAC candidate ids to associate with this topic"
    )


class TopicDescription(BaseModel):
    topic: str
    description: str = Field(..., description="Informative string for this topic")
    size: int


class TopicQuery(BaseModel):
    topic: str
    chunk_size: int = Field(
        100, gte=100, lte=10000, description="Number of alerts per chunk"
    )
    start: Optional[int] = Field(None, gte=0)
    stop: Optional[int] = Field(None, gte=1)
    step: Optional[int] = Field(None, gt=0)


class ConeConstraint(BaseModel):
    ra: float = Field(
        ..., description="Right ascension of field center in degrees (J2000)"
    )
    dec: float = Field(
        ..., description="Declination of field center in degrees (J2000)"
    )
    radius: float = Field(
        ..., gt=0, lt=180, description="Radius of search cone in degrees"
    )


class TimeConstraint(BaseModel):
    lt: Optional[float] = Field(None)
    gt: Optional[float] = Field(None)


class AlertQuery(BaseModel):
    cone: Optional[ConeConstraint] = None
    jd: TimeConstraint = TimeConstraint()
    programid: Optional[int] = None
    chunk_size: int = Field(
        100, gte=0, lte=10000, description="Number of alerts per chunk"
    )


class AlertCutouts(BaseModel):
    """
    Images are gzipped FITS files, b64 encoded
    """

    template: str
    science: str
    difference: str


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
    avro alert schema
    """

    fileName: str
    stampData: str


class Alert(BaseModel):
    """
    avro alert schema for ZTF (www.ztf.caltech.edu)
    """

    schemavsn: str = "3.3"
    publisher: str = "Ampel"
    objectId: str
    candid: int
    candidate: Candidate
    prv_candidates: Optional[List[PrvCandidate]]
    cutoutScience: Optional[Cutout]
    cutoutTemplate: Optional[Cutout]
    cutoutDifference: Optional[Cutout]


class AlertChunk(BaseModel):
    resume_token: str
    chunks_remaining: int
    alerts: List[Alert]
