from datacontract.imports.importer import Importer
from datacontract.model.data_contract_specification import DataContractSpecification
from datacontract.model.exceptions import DataContractException

class ContractModelImporter(Importer):
    def import_source(
        self, data_contract_specification: DataContractSpecification, source: str, import_args: dict
    ) -> DataContractSpecification:
        
        model = import_args.get('model')
        return import_contract_model(data_contract_specification, source=source, model=model)    


def import_contract_model(
        data_contract_spec: DataContractSpecification,
        source: str,
        model: str = 'all',
        ) -> DataContractSpecification:

    # Load the YAML file
    import_contract_spec = DataContractSpecification.from_file(source)

    for model_name, data_contract_model in import_contract_spec.models.items():

        if model != 'all' and model_name != model:
            continue

        adjustmed_model_name = model_name
        if model_name in data_contract_spec.models:
            suffix = 1
            while f"{model_name}_{suffix}" in data_contract_spec.models:
                suffix += 1

            adjustmed_model_name = f"{model_name}_{suffix}"
            model_config = data_contract_model.config or {}
            model_config.setdefault('sourceTable', model_name)
            data_contract_model.config = model_config

        data_contract_spec.models[adjustmed_model_name] = data_contract_model

    return data_contract_spec
