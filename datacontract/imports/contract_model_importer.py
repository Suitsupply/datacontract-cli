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

    if model == 'all':
        for model_name, data_contract_model in import_contract_spec.models.items():
            data_contract_spec.models[model_name] = data_contract_model
    else:
        if model not in import_contract_spec.models:
            raise DataContractException(
                type="schema",
                name="Import contract model",
                reason=f"Model '{model}' not found in the contract model specification.",
                engine="datacontract",
            )
        data_contract_spec.models[model] = import_contract_spec.models[model]

    return data_contract_spec
    