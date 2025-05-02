import json
import os

from datacontract.model.config_dbt import FieldConfigDBT, ModelConfigDBT
from datacontract.model.data_contract_specification import (
    DataContractSpecification as OriginalDataContractSpecification,
    Field as OriginalField,
    Model as OriginalModel,
)


class Field(OriginalField):
    """Extend the Field class to use FieldConfigDBT for the config property."""
    config: FieldConfigDBT | None = None


class Model(OriginalModel):
    """Extend the Model class to use ModelConfigDBT for the config property."""
    config: ModelConfigDBT | None = None
    fields: dict[str, Field] = {}


class DataContractSpecification(OriginalDataContractSpecification):
    """Extend the DataContractSpecification class to use the updated Field and Model."""
    models: dict[str, Model] = {}


def generate_custom_schema():
    """Generate a custom JSON schema with updated config properties."""
    # Generate the schema from the updated DataContractSpecification class
    base_schema = DataContractSpecification.model_json_schema()

    # Write the modified schema to disk
    output_path = os.path.join(os.path.dirname(__file__), 'data_contract_specification_dbt.json')
    with open(output_path, 'w') as f:
        json.dump(base_schema, f, indent=2)

    print(f"Custom schema with typed config fields saved to {output_path}")
    return output_path


if __name__ == "__main__":
    generate_custom_schema()
