# ETL and supporting scripts for C3DC Target Neuroblastoma (NBL) study

## Pre-requisites
Install Python (3.11 used at time of documentation) and add support for dependencies such as
[PETL](https://github.com/petl-developers/petl) by, for example,
[creating and activating a Conda environment](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file)
based on the [Conda environment file](https://github.com/chicagopcdc/c3dc_etl/blob/main/conda_environment.yml)

## Data harmonization process and input dependencies
The c3dc_etl.py script ingests the source data in tabular (XLSX) format, transforms it into a harmonized (JSON)
data file by applying the mapping rules in the [JSON transformation/mapping file](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/target/transformations)
and then performs validation against the [JSON schema version](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/schema.json)
of the [C3DC model](https://github.com/CBIIT/c3dc-model/tree/main/model-desc).

### C3DC model JSON schema
The data harmonization script uses the [C3DC JSON schema file](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/schema.json)
for reference and validation, which is converted from the [C3DC model's](https://github.com/CBIIT/c3dc-model/tree/main/model-desc)
source YAML files. See the [C3DC JSON schema readme file](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/README.md)
for more information.

### Mapping unpivoter utility script
The [mapping unpivoter script](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter) can be used to
transform the Excel (XLSX) document containing harmonized data mapping definitions to the
[publicly available JSON transformation/mapping deliverable](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/ccdi/transformations),
aka the 'remote' JSON configuration file described below referenced in the `transformations_url` property. It's
recommended to use the script in lieu of editing and maintaining the JSON transformation/mapping config file manually.
See the [README file](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/README.md) for configuration
and execution details.

## ETL execution
1. Create/update the [JSON schema version](https://github.com/chicagopcdc/c3dc_etl/tree/main/schema)
   of the [C3DC model](https://github.com/CBIIT/c3dc-model/tree/main/model-desc) if not using the publicly available
  [version](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/schema.json) published in GitHub by the D4CG team.
1. Create/update the [JSON transformation/mapping file](https://github.com/chicagopcdc/c3dc_etl/tree/main/mapping_unpivoter)
   for the study if not using the publicly available
   [version](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/target/transformations)
   published in GitHub by the D4CG team.
1. Create a local file named `.env` (see below for configuration details) as per the
   [example](https://github.com/chicagopcdc/c3dc_etl/blob/main/etl/target/.env_example) and execute the script
   without any arguments:
   ```
   python c3dc_etl.py
   ```
   A file with a name other than `.env` can be specified as a command line argument:
   ```
   python c3dc_etl.py "/path/to/config/file"
   ```

### Configuration
Configuration has been divided into local and remote file instances. The local configuration file is a .env file that
contains settings specific to the local runtime environment such as local paths for input/output files while the
remote configuration file is a JSON file containing the data transformation/mapping rules and is environment agnostic.
This separation allows the remote configuration files to be maintained in a version controlled repository such as
[GitHub](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/target/transformations) so that changes can be tracked
and audited.

The `STUDY_CONFIGURATIONS` variable of the local configuration file defines one or more study configuration entries
which in turn contain references to the remote configuration file (`transformations_url`) and a `transformations`
variable containing settings specific to each transformation for the study. Each local transformation must match a
transformation in the remote configuration file having the same `name` value.

The remote configuration file identified by the `transformations_url` variable of a local study configuration entry
defines a `STUDY_CONFIGURATION` object that contains transformations in which the data transformation/mapping rules
are specified for the corresponding source data file. The remote configuration file will be loaded and then the
transformation objects defined therein will be merged with transformation entries in the local study configuration
having the same `name` value to provide comprehensive transformation objects containing environment-specific settings
as well as data transformation/mapping rules needed by the ETL script. The remote configuration file can be created
using the `mapping unpivoter` script as described in that script's
[README file](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/README.md).

#### Local configuration
The sample [.env_example](https://github.com/chicagopcdc/c3dc_etl/blob/main/etl/target/.env_example) file can be used
as a starting point and customized as needed.
* `JSON_SCHEMA_URL`: Required. String containing the location of the JSON schema file that will validate the
  harmonized data file created by the ETL script. Can also be a local file path. For example
  `https://raw.githubusercontent.com/chicagopcdc/c3dc_etl/main/schema/schema.json` or `/path/to/schema.json`
* `STUDY_CONFIGURATIONS`: Required. A string-ified list of objects, with one configuration object per study.
  * `study`: The unique name or identifier for this study configuration object. This value must be unique within the
    list of study configuration objects. For example `phs000467`.
  * `active`: Optional. Boolean string (`True` or `False`, default `False`) that indicates whether this configuration
    object and the transformations specified within will be processed (`True`) or ignored (`False`).
  * `transformations_url`: Required. String specifying the location of the file containing the transformations
    configuration to be merged with the local transformation specified in the `transformations` config variable.
    Can also be a local file path. For example
    `https://raw.githubusercontent.com/chicagopcdc/c3dc_etl/main/etl/target/transformations/phs000467.json` or
    `/path/to/phs000467.json`.
  * `transformations`: Required. A string-ified list of objects, one per source data file, containing configuration
    details needed to harmonize each source data file. Each transformation will be matched and merged with an object
    in the remote config by `name`.
    * `name`: Required. String containing the unique name or identifier for this transformation. This value must be
      unique within the list of transformation and must match the name of the transformation specified in the
      transformation/mapping file located at `transformations_url`. For a transformation/mapping file created using
      the mapping unpivoter script, the name value would correspond to the `transformation_name` of the
      [environment variable](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/README.md#environment-variables).
      For example `phs000467`.
    * `source_file_path`: Required. String specifying the location of the file containing the source data. Can be a
      local path or AWS S3 URL. For example `/path/to/source/file` or `s3://bucket/path/to/source/file`.
    * `output_file_path`: Required. String specifying the location of the file where the harmonized data will be
      saved. Can be a local path or AWS S3 URL. For example `/path/to/harmonized/data/file` or
      `s3://bucket/path/to/harmonized/data/file`.
    * `uuid_seed`: Optional. String containing the seed value to be passed to the random number generator used by
      the internal UUID creation function called to provision record identifiers such as
      `reference_file.reference_file_id`. UUIDs will be deterministically reproducible across execution cycles if
      specified, otherwise they will be newly allocated with every script execution instance. For example `phs000467`.
    * `active`: Optional. Boolean string (`True` or `False`, default `False`) that indicates whether this
      transformation configuration object and associated mappings will be processed (`True`) or ignored (`False`).

#### Remote configuration (single `STUDY_CONFIGURATION` object that will be merged with local config object by matching transformation name)
The remote configuration JSON file can be created using the mapping unpivoter script as described in that script's
[README file](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/README.md).
* `version`: Optional. String containing the label identifying the version of this study configuration object. Value
  will be taken from the mapping unpivoter script's `VERSION` config variable for remote config files created by
  that script.
* `transformations`: A JSON array of objects, one per source file, containing configuration details needed
  to harmonize each source file. Each transformation will be matched and merged with an object in the local config
  by `name`.
  * `name`: Required. String containing the unique name or identifier for this transformation. There must be a
    local transformation configuration entry as detailed above with matching name that will be merged with this
    one to form the final configuration object used by the ETL script. Value will be taken from the
    mapping unpivoter script's `TRANSFORMATION_MAPPINGS_FILES` => `transformation_name` config variable for
    remote config files created by that script.
  * `mappings`: Required. A JSON array of mapping objects that specify how source data fields will be harmonized for
    output.  For remote config files created by the mapping unpivoter script, each mapping record will match the
    mappings defined for a given output field in the Excel (XLSX) mapping file specified in the
    `TRANSFORMATION_MAPPINGS_FILES` => `mappings_file` file.
    * `output_field`: Required. String containing the field, in `node_type.property_name` format, to which the
      harmonized data for this mapping will be saved. The node type and property name must correspond to node types and
      child properties specified in the [C3DC model](https://github.com/CBIIT/c3dc-model/blob/main/model-desc/c3dc-model.yml).
      Value will be taken from the Excel mapping document's `Target Variable Name` column for remote config files
      created by the mapping unpivoter script.
    * `source_field`: Required. String containing the source file field whose data value will be harmonized and saved
      to the `output_field`, for example `participant.sex_at_birth`, `diagnosis.age_at_diagnosis`, etc. The special
      value `"[string_literal]"` can be specified to indicate that the harmonized output value is derived from a static
      text value or function rather than a specific source file field. Multiple fields can be specified as a
      comma-separated list for use by 'macro-like' functions such as `{sum}` described below for `replacement_values`
      => `new_value`. Value will be taken from the Excel mapping document's `Source Variable Name` column for remote
      config files created by the mapping unpivoter script.
    * `type_group_index`: Optional. String containing an identifier that indicates if this field is for a multi-record
      mapping and, if so, how those mappings will be grouped together. A `type_group_index` value does not need to be
      specified for single record mappings, which are for fields associated with a single output record per source
      record such as `participant`. Every record in the source file that contains valid data will correspond to a
      single `participant` object in the harmonized data file and therefore field mappings for the `participant`
      object do not need to specify a value for `type_group_index`. Multi-record mappings are needed for
      `reference_file` output objects to account for the reference files involved in the ETL process such as the ETL
      script, JSON validation schema, JSON transformation/mapping specification file, and input source files. As a
      result there are multiple collections of field mappings for each `reference_file` object needed and a
      `type_group_index` value associated with each mapping group. The wildcard value `"*"` can be used to indicate
      mappings that apply to all records of that type such as `reference_file.reference_file_id`, for which all
      `reference_file` records will have their `reference_file_id` value set to a programmatically-generated UUID.
      Value will be taken from the Excel mapping document's `Type Group Index` column for remote config files created
      by the mapping unpivoter script.
    * `default_value`: Optional. String, integer or JSON array specifying the default value to which the `output_field`
      will be set when the input source data value of the field specified by `source_field` is blank/null. Value will
      be taken from the Excel mapping document's `Default Value If Null/Blank` column for remote config files created
      by the mapping unpivoter script.
    * `replacement_values`: Optional. A JSON array of `replacement_value` objects that specify how source data values
      will be harmonized. Each `replacement_value` pair will be processed in sequence and the harmonized output value
      will be set to `new_value` if the source data matches the value or criteria specifed in `old_value`. For remote
      config files created by the mapping unpivoter script, the `replacement_values` collection will correspond
      to the `Source Permissible Values Term` and `Target Permissible Values Term` column pairs for a given output
      field in the Excel mapping file specified in the `TRANSFORMATION_MAPPINGS_FILES` => `mappings_file` file. A
      replacement value entry will also be added for any items in the `Replacement Values` column of the Excel
      mapping file.
        * `old_value`: Required. String containing the old/existing value that will be mapped to the `new_value`.
          In addition to explicit values, the special characters `*` and `+` can be specified to indicate that the
          source data value will be replaced with `new_value` if the source data value is any value including null
          (`"*"`) or non-null (`"+"`). Partial wildcards such as `"prefix*"`, `"*suffix"`, and `"*contains*"` are NOT
          supported. Value will be taken from the Excel mapping document's `Source Permissible Values Term` column
          for remote config files created by the mapping unpivoter script.
        * `new_value`: Optional. String, or JSON array specyfing the value with which the source data will be
          replaced in the harmonized data output. Value will be taken from the Excel mapping document's
          `Target Permissible Values Term` column for remote config files created by the mapping unpivoter script.
          In addition to explicit values, the following special values are also allowed:
          * `"{field:source field name}"`: Replace with the specified source field value for the current source
            record, for example `"{field:TARGET USI}"`.
          * `"{find_enum_value}"`: Replace with the enum (aka permissible value list) entry found using the source
            field value as the key. For permissible list values containing the separator ` : ` the search will attempt
            to match on the code/prefix first and then the entire enum value. For example, for
            `diagnosis.anatomic_site`, the source value `C22.0` would match `C22.0 : Liver`. If there are multiple
            permissible list values with the same code/prefix then the **last** matching entry would be used as the
            replacement value.
          * `"{sum}"`: Replace with the sum of the values for the source fields specified in `source_field`.
          * `"{uuid}"`: Replace with a UUID (v4); see the description for `uuid_seed` config var for information
            on how UUIDs are generated.

## Sample ETL execution shell script
The commands below can be adapted and executed in a shell script such as `c3dc_etl.sh` for convenience to consolidate
all of the data harmonization steps for all studies into a single executable script.
```
# Creation of Target harmonized data file and transformation/mapping file involves the following steps:
# 1) If updating schema: update schema version var in ../../schema/.env and then either execute
#    ../../schema/schema_creator.py manually or uncomment schema script commands below
# 2) Update transformation/mapping version in ../../mapping_unpivoter/.env_mapping_unpivoter_[study_identifier]
# 3) Create transformation/mapping file using mapping unpivoter script
# 4) Run ETL to create harmonized data file

# activate conda env first with e.g. 'conda activate c3dc_etl'; check conda_environment.yml for package dependencies

# exit on error
set -e

# if updating schema, uncomment the following after updating the version var in the ../../schema.env file:
# cd ../../schema
# python schema_creator.py

# create log dirs if needed
mkdir -p logs
mkdir -p ../../mapping_unpivoter/logs

# declare an array of study identifiers that will match the names of the respective .env config files
# e.g. ".env_mapping_unpivoter_STUDY_IDENTIFIER" for the mapping unpivoter script and
# ".env_STUDY_IDENTIFIER" for the ETL script
declare -a studies=(
    "phs000467"
    "phs000471"
)

# enumerate each study to be harmonized; make sure version var has been updated if needed in
# ../../mapping_unpivoter/.env_mapping_unpivoter_[study_identifier]
for study in "${studies[@]}"
do
    echo "Harmonizing TARGET study \"$study\""
    # create transformation/mapping file; assuming starting dir is './etl/target' where '.' is project root dir
    cd ../../mapping_unpivoter
    python mapping_unpivoter.py unpivot_transformation_mappings .envs/".env_mapping_unpivoter_$study"
    cp mapping_unpivoter.log logs/"mapping_unpivoter_$study.log"

    # run ETL script to create harmonized data file
    cd ../etl/target
    python c3dc_etl.py .envs/".env_$study"
    cp c3dc_etl.log logs/"c3dc_etl_$study.log"
done
```
