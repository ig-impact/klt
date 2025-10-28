from __future__ import annotations
from typing import ClassVar
from dlt.common.libs.pydantic import DltConfig

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
)


class Sector(BaseModel):
    model_config = ConfigDict(
        extra="allow",
    )
    label: str | None = None
    value: str | None = None


class PiiCollection(BaseModel):
    model_config = ConfigDict(
        extra="allow",
    )
    label: str | None = None
    value: str | None = None


class CountryItem(BaseModel):
    model_config = ConfigDict(
        extra="allow",
    )
    label: str | None = None
    value: str | None = None


class Download(BaseModel):
    model_config = ConfigDict(
        extra="allow",
    )
    format: str | None = None
    url: str | None = Field(
        None,
        examples=["https://kf.kobotoolbox.org/api/v2/assets/aTPPUDScaFZkvBzd8FyK4Q/"],
    )


class Settings(BaseModel):
    model_config = ConfigDict(
        extra="allow",
    )
    sector: Sector | None = None
    country: list[CountryItem] | None = None
    description: str | None = None
    collects_pii: PiiCollection | None = None
    organization: str | None = None
    country_codes: list[str] | None = None
    operational_purpose: str | None = None


class ProjectViewAssetResponse(BaseModel):
    model_config = ConfigDict(
        extra="allow",
    )
    url: str = Field(
        ...,
        examples=[
            "https://kf.kobotoolbox.org/api/v2/project-views/pvyHWBnzRw3GCJpFs6cMdem/assets/"
        ],
    )
    date_created: AwareDatetime
    date_modified: AwareDatetime
    date_deployed: AwareDatetime
    owner: str = Field(
        ...,
        examples=[
            "https://kf.kobotoolbox.org/api/v2/project-views/pvyHWBnzRw3GCJpFs6cMdem/users/"
        ],
    )
    owner__username: str
    owner__email: EmailStr
    owner__name: str
    owner__organization: str
    uid: str
    name: str
    settings: Settings
    languages: list[str | None]
    has_deployment: bool
    deployment__active: bool
    deployment__submission_count: int
    deployment_status: str
    asset_type: str
    downloads: list[Download]
    owner_label: str


class PaginatedProjectViewAssetResponseList(BaseModel):
    model_config = ConfigDict(
        extra="allow",
    )
    count: int = Field(..., examples=[123])
    next: str | None = Field(
        None, examples=["http://api.example.org/accounts/?offset=400&limit=100"]
    )
    previous: str | None = Field(
        None, examples=["http://api.example.org/accounts/?offset=200&limit=100"]
    )
    results: list[ProjectViewAssetResponse]


class ProjectViewAssetResponseWithNesting(ProjectViewAssetResponse):
    dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}
