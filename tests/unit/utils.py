from argparse import Namespace
import os
import string
from unittest import TestCase, mock

import agate
from dbt.config.project import PartialProject


def mock_connection(name, state="open"):
    conn = mock.MagicMock()
    conn.name = name
    conn.state = state
    return conn


def config_from_parts_or_dicts(project, profile, packages=None, selectors=None, cli_vars={}):
    from dbt.config import Project, Profile, RuntimeConfig
    from copy import deepcopy

    if isinstance(project, Project):
        profile_name = project.profile_name
    else:
        profile_name = project.get("profile")

    if not isinstance(profile, Profile):
        profile = _profile_from_dict(
            deepcopy(profile),
            profile_name,
            cli_vars,
        )

    if not isinstance(project, Project):
        project = _project_from_dict(
            deepcopy(project),
            profile,
            packages,
            selectors,
            cli_vars,
        )

    args = Namespace(
        which="blah",
        single_threaded=False,
        vars=cli_vars,
        profile_dir="/dev/null",
    )
    return RuntimeConfig.from_parts(project=project, profile=profile, args=args)


def _profile_from_dict(profile, profile_name, cli_vars="{}"):
    from dbt.config import Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.config.utils import parse_cli_vars

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = ProfileRenderer(cli_vars)

    return Profile.from_raw_profile_info(
        profile,
        profile_name,
        renderer,
    )


def _project_from_dict(project, profile, packages=None, selectors=None, cli_vars="{}"):
    from dbt.config.renderer import DbtProjectYamlRenderer
    from dbt.config.utils import parse_cli_vars

    project_root = project.pop("project-root", os.getcwd())
    partial = PartialProject.from_dicts(
        project_root=project_root,
        project_dict=project,
        packages_dict=packages,
        selectors_dict=selectors,
    )

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = DbtProjectYamlRenderer(profile, cli_vars)
    project = partial.render(renderer)

    return project


def inject_adapter(adapter, plugin):
    """Inject the given adapter into the adapter factory, so your hand-crafted
    artisanal adapter will be available from get_adapter() as if dbt loaded it.
    """
    from dbt.adapters.factory import FACTORY

    plugin_key = plugin.adapter.type()
    FACTORY.plugins[plugin_key] = plugin

    adapter_key = adapter.type()
    FACTORY.adapters[adapter_key] = adapter


def clear_plugin(plugin):
    from dbt.adapters.factory import FACTORY

    adapter_key = plugin.adapter.type()
    FACTORY.plugins.pop(adapter_key, None)
    FACTORY.adapters.pop(adapter_key, None)


class TestAdapterConversions(TestCase):
    def _get_tester_for(self, column_type):
        from dbt_common.clients import agate_helper

        if column_type is agate.TimeDelta:  # dbt never makes this!
            return agate.TimeDelta()

        for instance in agate_helper.DEFAULT_TYPE_TESTER._possible_types:
            if isinstance(instance, column_type):  # include child types
                return instance

        raise ValueError(f"no tester for {column_type}")

    def _make_table_of(self, rows, column_types):
        column_names = list(string.ascii_letters[: len(rows[0])])
        if isinstance(column_types, type):
            column_types = [self._get_tester_for(column_types) for _ in column_names]
        else:
            column_types = [self._get_tester_for(typ) for typ in column_types]
        table = agate.Table(rows, column_names=column_names, column_types=column_types)
        return table


def load_internal_manifest_macros(config, macro_hook=lambda m: None):
    from dbt.parser.manifest import ManifestLoader

    return ManifestLoader.load_macros(config, macro_hook)
