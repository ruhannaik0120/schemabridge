"""Expose configured connection-profile labels without revealing credentials."""

from typing import Annotated

from fastapi import APIRouter, Depends

from schemabridge.services.profile_registry import ProfileRegistry

from ..dependencies import get_profile_registry
from ..schemas.common import ConnectionProfileListResponse, ConnectionProfileSummary


router = APIRouter(prefix="/api/v1/profiles", tags=["profiles"])


@router.get(
    "",
    operation_id="list_connection_profiles",
    summary="List credential-free configured connection profiles",
    response_model=ConnectionProfileListResponse,
)
async def list_connection_profiles(
    registry: Annotated[ProfileRegistry, Depends(get_profile_registry)],
) -> ConnectionProfileListResponse:
    """Return profile labels and safe selection metadata only."""

    profiles = registry.safe_profiles()
    return ConnectionProfileListResponse(
        items=[
            ConnectionProfileSummary(
                name=str(profile["name"]),
                db_type=str(profile["db_type"]),
                database=str(profile["database"]),
                database_present=bool(profile["database_present"]),
                write_enabled=bool(profile["write_enabled"]),
            )
            for profile in profiles
        ]
    )
