from typing import Sequence, Type

from dagster._core.definitions.asset_spec import AssetSpec

from .resource import PowerBIResource
from .translator import (
    DagsterPowerBITranslator,
    PowerBIContentData,
    PowerBIContentType,
    PowerBIWorkspaceData,
)


def fetch_powerbi_workspace_data(
    powerbi_resource: PowerBIResource,
) -> PowerBIWorkspaceData:
    """Retrieves all Power BI content from the workspace and returns it as a PowerBIWorkspaceData object.
    Future work will cache this data to avoid repeated calls to the Power BI API.

    Args:
        powerbi_resource (PowerBIResource): The Power BI resource to use to fetch the data.

    Returns:
        PowerBIWorkspaceData: A snapshot of the Power BI workspace's content.
    """
    dashboard_data = powerbi_resource.get_dashboards()["value"]
    augmented_dashboard_data = [
        {**dashboard, "tiles": powerbi_resource.get_dashboard_tiles(dashboard["id"])}
        for dashboard in dashboard_data
    ]
    dashboards = [
        PowerBIContentData(content_type=PowerBIContentType.DASHBOARD, properties=data)
        for data in augmented_dashboard_data
    ]

    reports = [
        PowerBIContentData(content_type=PowerBIContentType.REPORT, properties=data)
        for data in powerbi_resource.get_reports()["value"]
    ]
    semantic_models_data = powerbi_resource.get_semantic_models()["value"]
    data_sources_by_id = {}
    for dataset in semantic_models_data:
        dataset_sources = powerbi_resource.get_semantic_model_sources(dataset["id"])["value"]
        dataset["sources"] = [source["datasourceId"] for source in dataset_sources]
        for data_source in dataset_sources:
            data_sources_by_id[data_source["datasourceId"]] = PowerBIContentData(
                content_type=PowerBIContentType.DATA_SOURCE, properties=data_source
            )
    semantic_models = [
        PowerBIContentData(content_type=PowerBIContentType.SEMANTIC_MODEL, properties=dataset)
        for dataset in semantic_models_data
    ]
    return PowerBIWorkspaceData(
        dashboards_by_id={dashboard.properties["id"]: dashboard for dashboard in dashboards},
        reports_by_id={report.properties["id"]: report for report in reports},
        semantic_models_by_id={dataset.properties["id"]: dataset for dataset in semantic_models},
        data_sources_by_id=data_sources_by_id,
    )


def build_powerbi_asset_specs(
    *,
    powerbi_resource: PowerBIResource,
    dagster_powerbi_translator: Type[DagsterPowerBITranslator] = DagsterPowerBITranslator,
) -> Sequence[AssetSpec]:
    """Fetches Power BI content from the workspace and translates it into AssetSpecs,
    using the provided translator.
    Future work will cache this data to avoid repeated calls to the Power BI API.

    Args:
        powerbi_resource (PowerBIResource): The Power BI resource to use to fetch the data.
        dagster_powerbi_translator (Type[DagsterPowerBITranslator]): The translator to use
            to convert Power BI content into AssetSpecs. Defaults to DagsterPowerBITranslator.

    Returns:
        Sequence[AssetSpec]: A list of AssetSpecs representing the Power BI content.
    """
    workspace_data = fetch_powerbi_workspace_data(powerbi_resource)
    translator = dagster_powerbi_translator(context=workspace_data)

    all_content = [
        *workspace_data.dashboards_by_id.values(),
        *workspace_data.reports_by_id.values(),
        *workspace_data.semantic_models_by_id.values(),
        *workspace_data.data_sources_by_id.values(),
    ]

    return [translator.get_asset_spec(content) for content in all_content]
