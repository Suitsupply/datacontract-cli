from datacontract.export.exporter import Exporter
from datacontract.imports.dbt_specification_importer import DbtSpecificationImporter
from datacontract.model.exceptions import DataContractException

from datacontract.model.dbt_data_contract_specification import DataContractSpecification

from datacontract.export.dbt_bigquery.datasource_yml import datasource_yml
from datacontract.export.dbt_bigquery.raw_contract_sql import raw_contract_sql
from datacontract.export.dbt_bigquery.contract_yml import raw_contract_yml
from datacontract.export.dbt_bigquery.source_contract_sql import source_contract_sql
from datacontract.export.dbt_bigquery.contract_yml import source_contract_yml
from datacontract.export.dbt_bigquery.staging_contract_sql import staging_contract_sql
from datacontract.export.dbt_bigquery.dwh_view_sql import dwh_view_sql
from datacontract.export.dbt_bigquery.contract_yml import dwh_view_yml


class DbtSpecificationExporter(Exporter):

    dbt_specification = None

    CONTRACT_MAPPING = {
        'dbt-spec-raw-sql': raw_contract_sql,
        'dbt-spec-raw-yml': raw_contract_yml,
        'dbt-spec-data-source': datasource_yml,
        'dbt-spec-staging-sql': staging_contract_sql,
        'dbt-spec-source-sql': source_contract_sql,
        'dbt-spec-source-yml': source_contract_yml,
        'dbt-spec-dwh-view-sql': dwh_view_sql,
        'dbt-spec-dwh-view-yml': dwh_view_yml,
    }

    
    def export(self, data_contract: DataContractSpecification, model, server, sql_server_type, export_args) -> dict:
        
        model_name = model
        if (not model or model == 'all') and len(data_contract.models) == 1:
            model_name = next(iter(data_contract.models.models.keys()))

        #print(model_name)

        if model_name not in data_contract.models:
            raise DataContractException(
                type="schema",
                name="Export to dbt",
                reason=f"Model {model} not found in dbt specification.",
                engine="datacontract",
            )

        if not self.dbt_specification and data_contract.id.startswith('data_specification__'):
            self.dbt_specification = data_contract
        elif self.dbt_specification and model_name in self.dbt_specification.models:
            pass
        else:
            self.dbt_specification = DbtSpecificationExporter.get_dbt_specification(data_contract, model_name)

        print(self.dbt_specification.model_dump_json(indent=2))

        template = str(export_args.get("template"))
        if template is None:
            raise DataContractException(
                type="schema",
                name="Export to dbt",
                reason="Export to dbt requires template argument.",
                engine="datacontract",
            )
        
        if template in self.CONTRACT_MAPPING:
            func = self.CONTRACT_MAPPING.get(template)
        else:
            raise Exception('Unsupported template received')
        
        result = func(self.dbt_specification.models[model_name])
        return result
    

    @classmethod
    def get_dbt_specification(self, data_contract: DataContractSpecification, model) -> DataContractSpecification:
        
        import_args = {"model":model}
        dbt_specification_importer = DbtSpecificationImporter("dbt_specification")
        dbt_specification = dbt_specification_importer.import_source(data_contract_specification=data_contract, import_args=import_args)
                
        return dbt_specification
