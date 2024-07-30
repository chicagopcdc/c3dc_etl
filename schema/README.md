# C3DC JSON schema creator script

The C3DC data harmonization scripts use the
[C3DC model schema](https://github.com/CBIIT/c3dc-model/tree/main/model-desc) for reference and validation.
The `schema_creator.py` script converts the source YAML files into the required JSON schema format. Create/update
the `.env` file in the same directory and then execute `python schema_creator.py` to create the `schema.json` file.
The environment variables are described below; the sample
[.env_example](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/.env_example) can be used as a starting
point and customized as needed. A file with a name other than `.env` can be specified as a command line argument:
`python schema_creator.py "path/to/config/file"`

## Environment variables:
* `PROPS_SOURCE_URL`: Required. String containing the URL of the C3DC model properties file (`c3dc-model-props.yml`).
  For example `https://raw.githubusercontent.com/CBIIT/c3dc-model/main/model-desc/c3dc-model-props.yml`.
* `NODES_SOURCE_URL`: Required. String containing the URL of the C3DC model nodes file (`c3dc-model.yml`).
  For example `https://raw.githubusercontent.com/CBIIT/c3dc-model/main/model-desc/c3dc-model.yml`.
* `META_SCHEMA_URL`: Required. String containing the meta-schema URL declaring the version of the JSON Schema
  specification with which the output file is conformant. Mapped to the JSON schema output file's root element
  `$schema` property value. For example `https://json-schema.org/draft/2020-12/schema`.
* `SCHEMA_FILE_PATH`: Required. String containing the output path to which the JSON schema output file path will be
  written. For example `/path/to/schema.json`.
* `SCHEMA_ROOT_URL`: Required. String containing the JSON schema root URL, combined with `SCHEMA_ROOT_ID` to derive
  the JSON schema output file's root element `$id` property value that uniquely identifies the schema. For example
  `https://datascience.cancer.gov/schemas/c3dc`.
* `SCHEMA_ROOT_ID`: Required. String containing the JSON schema root id, combined with `SCHEMA_ROOT_URL` to derive
  the JSON schema output file's root element `$id` property value that uniquely identifes the schema. For example
  `2024-07-01`.
* `SCHEMA_ROOT_DESCRIPTION`: Optional. String containing the JSON schema description, mapped to the JSON output file's
  root element `description` property value. For example `C3DC schema bundle`.
* `SCHEMA_ROOT_COMMENT`: Optional. String containing the JSON schema comment, mapped to the JSON output file's root
  element `$comment` property value. For example `Based on v1.0.0 of C3DC model schema`
* `SCHEMA_ROOT_NODE`: Required. String containing the name of the root-level parent element of C3DC model nodes like
  `study`, `participant`, etc. For example `nodes`.
