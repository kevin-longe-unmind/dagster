import re
from collections import abc
from typing import TYPE_CHECKING, Any, Dict, Generic, Iterator, Sequence, Union, cast

from dagster import (
    AssetMaterialization,
    _check as check,
)
from dagster._annotations import experimental, public
from dagster._core.definitions.metadata.metadata_set import TableMetadataSet
from dagster._core.definitions.metadata.table import (
    TableColumn,
    TableColumnDep,
    TableColumnLineage,
    TableSchema,
)
from dagster._core.execution.context.asset_execution_context import AssetExecutionContext
from dagster._core.execution.context.op_execution_context import OpExecutionContext
from typing_extensions import TypeVar

if TYPE_CHECKING:
    from .resources import SlingResource


SlingEventType = AssetMaterialization

# We define DbtEventIterator as a generic type for the sake of type hinting.
# This is so that users who inspect the type of the return value of `DbtCliInvocation.stream()`
# will be able to see the inner type of the iterator, rather than just `DbtEventIterator`.
T = TypeVar("T", bound=SlingEventType)


def _get_logs_for_stream(
    stream_name: str,
    sling_cli: "SlingResource",
) -> Sequence[str]:
    corresponding_logs = []
    recording_logs = False
    for log in sling_cli.stream_raw_logs():
        if (f"running stream {stream_name}") in log:
            corresponding_logs.append(log)
            recording_logs = True
        elif recording_logs:
            if len(log.strip()) == 0:
                break
            corresponding_logs.append(log)
    return corresponding_logs


def _strip_quotes_target_table_name(target_table_name: str) -> str:
    return target_table_name.replace('"', "")


INSERT_REGEX: re.Pattern[str] = re.compile(r".*inserted (\d+) rows into (.*) in.*")
SLING_COLUMN_PREFIX = "_sling_"


def fetch_column_metadata(
    materialization: AssetMaterialization,
    sling_cli: "SlingResource",
    replication_config: Dict[str, Any],
    context: Union[OpExecutionContext, AssetExecutionContext],
) -> Dict[str, Any]:
    target_name = replication_config["target"]
    stream_name = cast(str, materialization.metadata["stream_name"].value)

    upstream_assets = set()
    if isinstance(context, AssetExecutionContext):
        upstream_assets = context.assets_def.asset_deps[materialization.asset_key]

    corresponding_logs = _get_logs_for_stream(stream_name, sling_cli)
    insert_log = next((log for log in corresponding_logs if re.match(INSERT_REGEX, log)), None)

    if insert_log:
        try:
            target_table_name = check.not_none(re.match(INSERT_REGEX, insert_log)).group(2)
            target_table_name = _strip_quotes_target_table_name(target_table_name)

            output = sling_cli.run_sling_cli(
                ["conns", "discover", target_name, "--pattern", target_table_name, "--columns"]
            )
            table_rows = [row.strip()[1:-1] for row in output.split("\n") if row.startswith("|")]
            tabular_data = [[x.strip() for x in row.split("|")] for row in table_rows]

            col_name_idx = tabular_data[0].index("COLUMN")
            general_type_idx = tabular_data[0].index("GENERAL TYPE")

            column_type_map = {row[col_name_idx]: row[general_type_idx] for row in tabular_data[1:]}

            column_lineage = None
            # If there is only one upstream asset (typical case), we can infer column lineage
            # from the single upstream asset which is being replicated exactly.
            if len(upstream_assets) == 1:
                upstream_asset_key = next(iter(upstream_assets))
                column_lineage = TableColumnLineage(
                    deps_by_column={
                        column_name: [
                            TableColumnDep(
                                asset_key=upstream_asset_key,
                                column_name=column_name,
                            )
                        ]
                        for column_name in column_type_map.keys()
                        if not column_name.startswith(SLING_COLUMN_PREFIX)
                    }
                )

            return dict(
                TableMetadataSet(
                    column_schema=TableSchema(
                        columns=[
                            TableColumn(name=column_name, type=column_type)
                            for column_name, column_type in column_type_map.items()
                        ]
                    ),
                    column_lineage=column_lineage,
                )
            )
        except Exception as e:
            context.log.warning("Failed to fetch column metadata for stream %s: %s", stream_name, e)

    return {}


class SlingEventIterator(Generic[T], abc.Iterator):
    """A wrapper around an iterator of ling events which contains additional methods for
    post-processing the events, such as fetching column metadata.
    """

    def __init__(
        self,
        events: Iterator[T],
        sling_cli: "SlingResource",
        replication_config: Dict[str, Any],
        context: Union[OpExecutionContext, AssetExecutionContext],
    ) -> None:
        self._inner_iterator = events
        self._sling_cli = sling_cli
        self._replication_config = replication_config
        self._context = context

    def __next__(self) -> T:
        return next(self._inner_iterator)

    def __iter__(self) -> "SlingEventIterator[T]":
        return self

    @experimental
    @public
    def fetch_column_metadata(self) -> "SlingEventIterator":
        """Fetches column metadata for each table synced by the Sling CLI.

        Retrieves the column schema and lineage for each target table.

        Returns:
            SlingEventIterator: An iterator of Dagster events with column metadata attached.
        """

        def _fetch_column_metadata() -> Iterator[T]:
            for event in self:
                col_metadata = fetch_column_metadata(
                    event, self._sling_cli, self._replication_config, self._context
                )
                yield event.with_metadata({**col_metadata, **event.metadata})

        return SlingEventIterator[T](
            _fetch_column_metadata(), self._sling_cli, self._replication_config, self._context
        )
