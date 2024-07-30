from dagster._core.libraries import DagsterLibraryRegistry

from .asset_specs import build_powerbi_asset_specs as build_powerbi_asset_specs
from .resource import PowerBIResource as PowerBIResource
from .translator import DagsterPowerBITranslator as DagsterPowerBITranslator
from .version import __version__

DagsterLibraryRegistry.register("dagster-powerbi", __version__)
