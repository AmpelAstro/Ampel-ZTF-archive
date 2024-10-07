import math
from base64 import b64encode
from datetime import datetime
from typing import Annotated, Any, Literal, Optional, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    field_validator,
    model_validator,
)

from ..ArchiveDBClient import ArchiveDBClient
from ..types import FilterClause


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Stream(BaseModel):
    resume_token: str
    chunk_size: int


class ChunkCount(BaseModel):
    items: int
    chunks: int


class StreamDescription(Stream):
    remaining: ChunkCount
    pending: ChunkCount
    started_at: datetime
    finished_at: Optional[datetime] = None


class Topic(BaseModel):
    description: str = Field(..., description="Informative string for this topic")
    candids: list[int] = Field(
        ..., description="IPAC candidate ids to associate with this topic"
    )


class TopicDescription(BaseModel):
    topic: str
    description: str = Field(..., description="Informative string for this topic")
    size: int


class TopicQuery(StrictModel):
    topic: str
    chunk_size: int = Field(
        100, ge=100, le=10000, description="Number of alerts per chunk"
    )
    start: Optional[int] = Field(None, ge=0)
    stop: Optional[int] = Field(None, ge=1)
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


class AlertQuery(CandidateFilterable):
    cone: Optional[ConeConstraint] = None
    jd: TimeConstraint = TimeConstraint()  # type: ignore[call-arg]
    candidate: Optional[FilterClause] = None
    chunk_size: int = Field(
        100, ge=0, le=10000, description="Number of alerts per chunk"
    )

    @model_validator(mode="before")
    @classmethod
    def at_least_one_constraint(cls, values: Any):
        if isinstance(values, dict) and not {"cone", "jd"}.intersection(values.keys()):
            raise ValueError("At least one constraint (cone or jd) must be specified")
        return values


class ObjectQuery(CandidateFilterable):
    objectId: Union[str, list[str]]
    jd: TimeConstraint = TimeConstraint()  # type: ignore[call-arg]
    candidate: Optional[FilterClause] = None
    chunk_size: int = Field(
        100, ge=0, le=10000, description="Number of alerts per chunk"
    )


class AlertChunkQueryBase(StrictModel):
    """Options for queries that will return a chunk of alerts"""

    latest: bool = Field(
        False, description="Return only the latest alert for each objectId"
    )
    with_history: bool = False
    with_cutouts: bool = False
    chunk_size: int = Field(
        100, gt=0, le=10000, description="Number of alerts to return per page"
    )
    resume_token: Optional[str] = Field(
        None,
        description="Identifier of a previous query to continue. This token expires after 24 hours.",
    )


class MapQueryBase(CandidateFilterable):
    jd: StrictTimeConstraint


class HEALpixMapRegion(StrictModel):
    nside: int = Field(..., gt=0, le=ArchiveDBClient.NSIDE)
    pixels: list[int]

    @field_validator("nside")
    @classmethod
    def power_of_two(cls, nside):
        if not math.log2(nside).is_integer():
            raise ValueError("nside must be a power of 2")
        return nside


class HEALpixMapQuery(AlertChunkQueryBase, MapQueryBase, HEALpixMapRegion): ...


class HEALpixRegionQueryBase(MapQueryBase):
    regions: list[HEALpixMapRegion]


class HEALpixRegionQuery(AlertChunkQueryBase, HEALpixRegionQueryBase): ...


class HEALpixRegionCountQuery(HEALpixRegionQueryBase): ...


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
    diffmaglim: Optional[float] = None
    pdiffimfilename: Optional[str] = None
    programpi: Optional[str] = None
    programid: Literal[1, 2, 3]
    candid: int
    isdiffpos: str
    tblid: int
    nid: Optional[int] = None
    rcid: Optional[int] = None
    field: Optional[int] = None
    xpos: Optional[float] = None
    ypos: Optional[float] = None
    ra: float
    dec: float
    magpsf: float
    sigmapsf: float
    chipsf: Optional[float] = None
    magap: Optional[float] = None
    sigmagap: Optional[float] = None
    distnr: Optional[float] = None
    magnr: Optional[float] = None
    sigmagnr: Optional[float] = None
    chinr: Optional[float] = None
    sharpnr: Optional[float] = None
    sky: Optional[float] = None
    magdiff: Optional[float] = None
    fwhm: Optional[float] = None
    classtar: Optional[float] = None
    mindtoedge: Optional[float] = None
    magfromlim: Optional[float] = None
    seeratio: Optional[float] = None
    aimage: Optional[float] = None
    bimage: Optional[float] = None
    aimagerat: Optional[float] = None
    bimagerat: Optional[float] = None
    elong: Optional[float] = None
    nneg: Optional[int] = None
    nbad: Optional[int] = None
    rb: Optional[float] = None
    ssdistnr: Optional[float] = None
    ssmagnr: Optional[float] = None
    ssnamenr: Optional[str] = None
    sumrat: Optional[float] = None
    magapbig: Optional[float] = None
    sigmagapbig: Optional[float] = None
    ranr: float
    decnr: float
    sgmag1: Optional[float] = None
    srmag1: Optional[float] = None
    simag1: Optional[float] = None
    szmag1: Optional[float] = None
    sgscore1: Optional[float] = None
    distpsnr1: Optional[float] = None
    ndethist: int
    ncovhist: int
    jdstarthist: Optional[float] = None
    jdendhist: Optional[float] = None
    scorr: Optional[float] = None
    tooflag: Optional[int] = None
    objectidps1: Optional[int] = None
    objectidps2: Optional[int] = None
    sgmag2: Optional[float] = None
    srmag2: Optional[float] = None
    simag2: Optional[float] = None
    szmag2: Optional[float] = None
    sgscore2: Optional[float] = None
    distpsnr2: Optional[float] = None
    objectidps3: Optional[int] = None
    sgmag3: Optional[float] = None
    srmag3: Optional[float] = None
    simag3: Optional[float] = None
    szmag3: Optional[float] = None
    sgscore3: Optional[float] = None
    distpsnr3: Optional[float] = None
    nmtchps: int
    rfid: int
    jdstartref: float
    jdendref: float
    nframesref: int
    rbversion: Optional[str] = None
    dsnrms: Optional[float] = None
    ssnrms: Optional[float] = None
    dsdiff: Optional[float] = None
    magzpsci: Optional[float] = None
    magzpsciunc: Optional[float] = None
    magzpscirms: Optional[float] = None
    nmatches: Optional[int] = None
    clrcoeff: Optional[float] = None
    clrcounc: Optional[float] = None
    zpclrcov: Optional[float] = None
    zpmed: Optional[float] = None
    clrmed: Optional[float] = None
    clrrms: Optional[float] = None
    neargaia: Optional[float] = None
    neargaiabright: Optional[float] = None
    maggaia: Optional[float] = None
    maggaiabright: Optional[float] = None
    exptime: Optional[float] = None
    drb: Optional[float] = None
    drbversion: Optional[str] = None


class PrvCandidate(BaseModel):
    """
    avro alert schema
    """

    jd: float
    fid: int
    pid: int
    diffmaglim: Optional[float] = None
    pdiffimfilename: Optional[str] = None
    programpi: Optional[str] = None
    programid: int
    candid: Optional[int] = None
    isdiffpos: Optional[str] = None
    tblid: Optional[int] = None
    nid: Optional[int] = None
    rcid: Optional[int] = None
    field: Optional[int] = None
    xpos: Optional[float] = None
    ypos: Optional[float] = None
    ra: Optional[float] = None
    dec: Optional[float] = None
    magpsf: Optional[float] = None
    sigmapsf: Optional[float] = None
    chipsf: Optional[float] = None
    magap: Optional[float] = None
    sigmagap: Optional[float] = None
    distnr: Optional[float] = None
    magnr: Optional[float] = None
    sigmagnr: Optional[float] = None
    chinr: Optional[float] = None
    sharpnr: Optional[float] = None
    sky: Optional[float] = None
    magdiff: Optional[float] = None
    fwhm: Optional[float] = None
    classtar: Optional[float] = None
    mindtoedge: Optional[float] = None
    magfromlim: Optional[float] = None
    seeratio: Optional[float] = None
    aimage: Optional[float] = None
    bimage: Optional[float] = None
    aimagerat: Optional[float] = None
    bimagerat: Optional[float] = None
    elong: Optional[float] = None
    nneg: Optional[int] = None
    nbad: Optional[int] = None
    rb: Optional[float] = None
    ssdistnr: Optional[float] = None
    ssmagnr: Optional[float] = None
    ssnamenr: Optional[str] = None
    sumrat: Optional[float] = None
    magapbig: Optional[float] = None
    sigmagapbig: Optional[float] = None
    ranr: Optional[float] = None
    decnr: Optional[float] = None
    scorr: Optional[float] = None
    magzpsci: Optional[float] = None
    magzpsciunc: Optional[float] = None
    magzpscirms: Optional[float] = None
    clrcoeff: Optional[float] = None
    clrcounc: Optional[float] = None
    rbversion: Optional[str] = None


class FPHist(BaseModel):
    field: Optional[int] = None
    rcid: Optional[int] = None
    fid: int
    pid: int
    rfid: int
    sciinpseeing: Optional[float] = None
    scibckgnd: Optional[float] = None
    scisigpix: Optional[float] = None
    magzpsci: Optional[float] = None
    magzpsciunc: Optional[float] = None
    magzpscirms: Optional[float] = None
    clrcoeff: Optional[float] = None
    clrcounc: Optional[float] = None
    exptime: Optional[float] = None
    adpctdif1: Optional[float] = None
    adpctdif2: Optional[float] = None
    diffmaglim: Optional[float] = None
    programid: int
    jd: float
    forcediffimflux: Optional[float] = None
    forcediffimfluxunc: Optional[float] = None
    procstatus: Optional[str] = None
    distnr: Optional[float] = None
    ranr: float
    decnr: float
    magnr: Optional[float] = None
    sigmagnr: Optional[float] = None
    chinr: Optional[float] = None
    sharpnr: Optional[float] = None


class Cutout(BaseModel):
    """
    stampData is a gzipped FITS file, b64 encoded
    """

    fileName: str
    # NB: ser_json_bytes="base64" uses a URL-safe alphabet, whereas b64encode
    # uses +/ to represent 62 and 63. Use a serialization function to emit
    # strings that can be properly decoded with b64decode. See:
    # https://github.com/pydantic/pydantic/issues/7000
    stampData: Annotated[
        bytes,
        PlainSerializer(lambda v: b64encode(v), return_type=bytes, when_used="json"),
    ]


class AlertBase(BaseModel):
    candid: int
    objectId: str
    model_config = ConfigDict(
        ser_json_bytes="base64",
        val_json_bytes="base64",
    )


class AlertCutouts(AlertBase):
    cutoutScience: Optional[Cutout] = None
    cutoutTemplate: Optional[Cutout] = None
    cutoutDifference: Optional[Cutout] = None


class Alert_33(AlertCutouts):
    """
    avro alert schema for ZTF (www.ztf.caltech.edu)
    """

    schemavsn: Union[
        Literal["1.9"],
        Literal["2.0"],
        Literal["3.0"],
        Literal["3.1"],
        Literal["3.2"],
        Literal["3.3"],
    ]
    publisher: str = "Ampel"
    candidate: Candidate
    prv_candidates: Optional[list[PrvCandidate]] = None


class Alert_402(Alert_33):
    schemavsn: Literal["4.02"]  # type: ignore[assignment]
    fp_hists: Optional[list[FPHist]] = None


Alert = Union[Alert_33, Alert_402]


class AlertChunk(BaseModel):
    resume_token: str
    chunk: Optional[int] = None
    alerts: list[Alert]
    remaining: ChunkCount
    pending: ChunkCount
    model_config = ConfigDict(
        ser_json_bytes="base64",
        val_json_bytes="base64",
    )


class AlertCount(BaseModel):
    count: int
