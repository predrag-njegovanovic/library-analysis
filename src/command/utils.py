from importlib import import_module
from os import environ
from pathlib import Path

from src.common import App, Config, init_reader, init_writer, load_config_module

DEFAULT_CONFIG_PATH = (
    Path(__file__).absolute().parent.parent / "config" / "settings.toml"
)


def application_name(full_app_name: str) -> tuple[str, str]:
    """Split full application name to module and application parts.

    Args:
        full_app_name (str): Application name with module.

    Raises:
        NameError: Application name not in format module.application_name

    Returns:
        Tuple: Pair of module_name, application_name
    """
    names = full_app_name.rsplit(".", maxsplit=1)
    if len(names) != 2:
        raise NameError(f"Invalid application name: '{full_app_name}'")

    return names[0], names[1]


def load_app(config: Config, config_module: str, full_app_name: str) -> App:
    module_name, app_name = application_name(full_app_name)
    config_module = load_config_module(config, config_module)
    reader = init_reader(config_module)
    writer = init_writer(config_module)

    return getattr(import_module(module_name), app_name)(config_module, reader, writer)


def set_root_data_dir() -> None:
    root_dir = Path(__file__).absolute().parent.parent.parent
    data_dir = root_dir / "data"

    environ["ROOT_DATA_DIR"] = str(data_dir)
