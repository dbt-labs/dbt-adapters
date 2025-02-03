from typing import Dict, List

from dbt.adapters.base import available


@available
def standardize_grants_dict(grants_table: "agate.Table") -> dict:
    """Translate the result of `show grants` (or equivalent) to match the
    grants which a user would configure in their project.

    Ideally, the SQL to show grants should also be filtering:
    filter OUT any grants TO the current user/role (e.g. OWNERSHIP).
    If that's not possible in SQL, it can be done in this method instead.

    :param grants_table: An agate table containing the query result of
        the SQL returned by get_show_grant_sql
    :return: A standardized dictionary matching the `grants` config
    :rtype: dict
    """
    grants_dict: Dict[str, List[str]] = {}
    for row in grants_table:
        grantee = row["grantee"]
        privilege = row["privilege_type"]
        if privilege in grants_dict.keys():
            grants_dict[privilege].append(grantee)
        else:
            grants_dict.update({privilege: [grantee]})
    return grants_dict
