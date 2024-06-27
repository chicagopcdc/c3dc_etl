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
* `PROPS_SOURCE_URL`: URL of C3DC model properties file, `c3dc-model-props.yml`
* `NODES_SOURCE_URL`: URL of C3DC model nodes file, `c3dc-model.yml`
* `META_SCHEMA_URL`: Meta-schema URL, mapped to JSON schema output file's `$schema` property value
* `SCHEMA_FILE_PATH`: JSON schema output file path
* `SCHEMA_ROOT_URL`: JSON schema root URL, combined with `SCHEMA_ROOT_ID` to determine `$id` property value
* `SCHEMA_ROOT_ID`: JSON schema root id, combined with `SCHEMA_ROOT_URL` to determine `$id` property value
* `SCHEMA_ROOT_DESCRIPTION`: JSON schema description, mapped to `$description` property value
* `SCHEMA_ROOT_COMMENT`: JSON schema comment, mapped to `$comment` property value
* `SCHEMA_ROOT_NODE`: Name of root-level parent element of C3DC model nodes like `study`, `participant`, etc
