from dagster_shared.libraries import DagsterLibraryRegistry

from dagster_tenki.pipes import (
    PipesTenkiClient as PipesTenkiClient,
    PipesTenkiMessageReader as PipesTenkiMessageReader,
)
from dagster_tenki.version import __version__

DagsterLibraryRegistry.register("dagster-tenki", __version__)
