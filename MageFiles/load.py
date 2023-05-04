from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(data, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    table_id = 'uber_data_engineering.fact_table'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        Dataframe(data['fact_table']),
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
    )
	
	
	https://lookerstudio.google.com/reporting/676ee7ba-9ceb-4741-8d06-ab5a2fb2d9b3
	https://lucid.app/lucidchart/10ec5934-2d59-43cb-b537-30c1deeb4919/edit?invitationId=inv_648fe600-99ac-4498-91c6-3db288bf9e6e
