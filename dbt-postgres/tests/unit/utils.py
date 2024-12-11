from argparse import Namespace
import os
from unittest import mock

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
    """
    Inject the given adapter into the factory
    so that it will be available from get_adapter() as if dbt loaded it.
    """
    from dbt.adapters.factory import FACTORY

    plugin_key = plugin.adapter.type()
    FACTORY.plugins[plugin_key] = plugin

    adapter_key = adapter.type()
    FACTORY.adapters[adapter_key] = adapter


def clear_plugin(plugin):
    """
    Remove the adapter on the given plugin from the factory.
    """
    from dbt.adapters.factory import FACTORY

    adapter_key = plugin.adapter.type()
    FACTORY.plugins.pop(adapter_key, None)
    FACTORY.adapters.pop(adapter_key, None)


def load_internal_manifest_macros(config, macro_hook=lambda m: None):
    from dbt.parser.manifest import ManifestLoader

    return ManifestLoader.load_macros(config, macro_hook)
