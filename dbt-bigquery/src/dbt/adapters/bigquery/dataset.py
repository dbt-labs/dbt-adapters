from typing import List

from google.cloud.bigquery import AccessEntry, Dataset

from dbt.adapters.events.logging import AdapterLogger


logger = AdapterLogger("BigQuery")


def _access_entries_match(existing_entry: AccessEntry, access_entry: AccessEntry) -> bool:
    """Check if two access entries match based on role, entity_type, and properties.

    We can't simply use equality because the locally created AccessEntry can have extra properties.
    """
    role_match = existing_entry.role == access_entry.role
    entity_type_match = existing_entry.entity_type == access_entry.entity_type
    property_match = existing_entry._properties.items() <= access_entry._properties.items()
    return role_match and entity_type_match and property_match


def is_access_entry_in_dataset(dataset: Dataset, access_entry: AccessEntry) -> bool:
    """Check if the access entry already exists in the dataset.

    Args:
        dataset (Dataset): the dataset to be updated
        access_entry (AccessEntry): the access entry to be added to the dataset

    Returns:
        bool: True if entry exists in dataset, False otherwise
    """
    return any(
        _access_entries_match(existing, access_entry)
        for existing in dataset.access_entries
    )


def remove_access_entry_from_dataset(dataset: Dataset, access_entry: AccessEntry) -> Dataset:
    """Removes a matching access entry from a dataset.

    Args:
        dataset (Dataset): the dataset to be updated
        access_entry (AccessEntry): the access entry to be removed from the dataset

    Returns:
        Dataset: the updated dataset
    """
    dataset.access_entries = [
        existing for existing in dataset.access_entries
        if not _access_entries_match(existing, access_entry)
    ]
    return dataset


def add_access_entry_to_dataset(dataset: Dataset, access_entry: AccessEntry) -> Dataset:
    """Adds an access entry to a dataset, always use access_entry_present_in_dataset to check
    if the access entry already exists before calling this function.

    Args:
        dataset (Dataset): the dataset to be updated
        access_entry (AccessEntry): the access entry to be added to the dataset

    Returns:
        Dataset: the updated dataset
    """
    access_entries: List[AccessEntry] = dataset.access_entries
    access_entries.append(access_entry)
    dataset.access_entries = access_entries
    return dataset
