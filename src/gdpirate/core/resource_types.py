import logging

from gdpirate.core.models import ResourceType

logger = logging.getLogger(__name__)

SPECIFICITY = {
    ResourceType.UNKNOWN: 0,
    ResourceType.FILE: 1,
    ResourceType.DOCUMENT: 2,
    ResourceType.SPREADSHEET: 2,
    ResourceType.PRESENTATION: 2,
    ResourceType.FORM: 2,
    ResourceType.DRAWING: 2,
    ResourceType.FOLDER: 2,
}


def should_upgrade_resource_type(
    existing: ResourceType, discovered: ResourceType
) -> bool:
    return SPECIFICITY[discovered] > SPECIFICITY[existing]


def is_contradictory_type(existing: ResourceType, discovered: ResourceType) -> bool:
    return (
        existing != discovered
        and SPECIFICITY[existing] == SPECIFICITY[discovered]
        and SPECIFICITY[existing] > 0
    )
