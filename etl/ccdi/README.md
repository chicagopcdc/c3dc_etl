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
of the [C3DC model](https://github.com/CBIIT/c3dc-model/tree/main/model-desc). CCDI input source files are Excel
(XLSX) workbooks based on the [CCDI submission template available in GitHub](https://github.com/CBIIT/ccdi-model/tree/main/metadata-manifest).
It is expected that records for [C3DC model nodes](https://github.com/CBIIT/c3dc-model/blob/main/model-desc/c3dc-model.yml)
such as `study`, `participant`, `diagnosis`, etc will be found worksheets havings matching names, with one record
per row. The data harmonization logic is implemented in a common ETL script (and JSON schema) while each individual
study will have a separate [transformation mapping file](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/ccdi/transformations).

### C3DC model JSON schema
The data harmonization script uses the [C3DC JSON schema file](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/schema.json)
for reference and validation, which is converted from the [C3DC model's](https://github.com/CBIIT/c3dc-model/tree/main/model-desc)
source YAML files. See the [C3DC JSON schema readme file](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/README.md)
for more information.

### Mapping unpivoter utility script
The [mapping unpivoter script](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter) can be used to
transform the development team's internal shared document containing harmonized data mapping definitions to the
[publicly available JSON transformation/mapping deliverable](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/target/transformations),
aka the 'remote' configuration file described below referenced in the `transformations_url` property. The script can
be used for convenience in lieu of editing and maintaining the JSON transformation/mapping config file manually. See
the [readme file](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/README.md) for details.

## ETL execution
1. Create/update the [JSON schema version](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/schema.json)
of the [C3DC model](https://github.com/CBIIT/c3dc-model/tree/main/model-desc) if needed.
1. Create/update the [JSON transformation/mapping file](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/target/transformations)
   if needed.
1. Create a local file named `.env` (see below for configuration details) as per the
   [example](https://github.com/chicagopcdc/c3dc_etl/blob/main/etl/ccdi/.env_example) and execute the script
   without any arguments:
   ```
   python c3dc_etl.py
   ```
   A file with a name other than `.env` can be specified as a command line argument:
   ```
   python c3dc_etl.py "/path/to/config/file"
   ```

### Configuration
Configuration has been divided into local and remote file instances. The local configuration file contains settings
specific to the local runtime environment such as local paths for input/output files. The remote configuration file,
whose location is specified in the `transformations_url` environment variable below, defines a `STUDY_CONFIGURATION`
object that contains the harmonized data mappings for the corresponding source data file and can be maintained in a
version controlled repository so that changes can be tracked and audited as needed. The remote configuration file
will be loaded and then the resulting `STUDY_CONFIGURATION` object will be merged with the matching local
`STUDY_CONFIGURATION` object to configure the ETL script.

#### Local configuration
* `JSON_SCHEMA_URL`: The location of the JSON schema file that will validate the harmonized data file created by
    the script.
* `STUDY_CONFIGURATIONS`: A string-ified list of objects, with one configuration object per study.
  * `study`: The unique name or identifier for this study configuration object.
  * `active`: Whether this configuration object and the transformations specified within will be processed (true)
    or ignored (false).
  * `transformations_url`: The location of the file containing the transformations configuration to be merged with
    the 'local' transformation specified in the `transformations` var.
  * `transformations`: A list of objects, one per source file, containing configuration details needed to harmonize
    each source file
    * `name`: The unique name or identifier for this transformation. This must match the name of the transformation
      specified in the transformation/mapping file located at `transformations_url`. For a file created using the
      [mapping_unpivoter script](https://github.com/chicagopcdc/c3dc_etl/tree/main/mapping_unpivoter), the name
      value would correspond to the `transformation_name` environment variable of the
      [mapping_unpivoter script](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/README.md#environment-variables).
    * `source_file_path`: The local path to the file containing the source data.
    * `output_file_path`: The local path to the file where the harmonized data will be saved.
    * `uuid_seed`: The optional seed to be passed to the random number generator used by the internal UUID creation
      function used to provision record identifiers such as `participant.participant_id`. UUIDs will be generated
      consistently across execution cycles if specified, otherwise they will be newly allocated with every script
      execution instance.
    * `active`: Whether this configuration object and the transformations specified within will be processed (true)
        or ignored (false).

#### Remote configuration (single `STUDY_CONFIGURATION` object that will be merged with local config object by matching transformation name)
* `version`: The label identifying the version of this study configuration object.
* `transformations`: A list of objects, one per source file, containing configuration details needed to harmonize
  each source file.
  * `name`: The unique name or identifier for this transformation. There must be a 'local' transformation
    configuration object as detailed above with matching name that will be merged with this one to form the final
    configuration object used by the ETL script.
  * `mappings`: The list of mapping objects that specify how source data fields will be harmonized for output.
    * `output_field`: The field, in `node_type.property_name` format, to which the harmonized data for this mapping
      will be saved. The node type and property name must correspond to node types and child properties specified in
      the [C3DC model](https://github.com/CBIIT/c3dc-model/blob/main/model-desc/c3dc-model.yml).
    * `source_field`: The source file field whose data value will be harmonized and saved to the `output_field`, e.g.
        `TARGET USI`, `Age at Diagnosis in Days`, `INSS Stage`, etc. The special value `[string_literal]` can be
        specified to indicate that the harmonized output value is derived from a static text value rather than a
        specific source file field. Multiple fields can be specified as a comma-separated
        ([CSV rules](https://docs.python.org/3/library/csv.html)) list for use by 'macro-like' functions such as
        `{sum}` as described below.
    * `type_group_index`: An identifier that indicates if this field is for a multi-record mapping and, if so, how
      those mappings will be grouped together. This var can be omitted for single record mappings, which are for
      fields associated with a single output record per source record such as `participant`. Every record in the
      source file that contains valid data will correspond with a single `participant` object in the harmonized data
      file and therefore field mappings for the `participant` object do not need to specify a value for
      `type_group_index` var. Multi-record mappings are needed for `reference_file` output objects to account for the
      reference files involved in the ETL process such as the ETL script, JSON validation schema, JSON
      transformation/mapping specification file, and input source files. As a result there are multiple collections
      of field mappings for each `reference_file` object needed and a `type_group_index` value associated with each
      mapping group. The wildcard value `*` can be used to indicate mappings that apply to all records of that type
      such as `reference_file.reference_file_id`, for which all `reference_file` records will have their
      `reference_file_id` value set to a programmatically-generated UUID.
    * `default_value`: The default value to set the `output_field` to when the `source_field` value is blank/null.
    * `replacement_values`: A list of `replacement_value` objects that specify how 'old' source data values will be
      harmonized. Each `replacement_value` pair will be processed in sequence and the harmonized output value will be
      set to `new_value` if the source data matches the value or criteria specifed in `old_value`.
        * `old_value`: The old/existing value that will be mapped to the `new_value`. In addition to explicit values,
          the special characters `*` and `+` can be specified to indicate that the source data value will be
          replaced with `new_value` if the source data value is anything including null (`*`) or non-null (`+`).
          Partial wildcards such as `prefix*`, `*suffix`, and `*contains*` are NOT supported.
        * `new_value`: The value with which the source data should be replaced in the harmonized data output. In
          addition to explicit values, the following special values are also allowed:  
          * `{field:source field name}`: substitute with the specified source field value for the current source
            record, for example `{field:TARGET USI}`.
          * `{sum}`: substitute with the sum of the values for the source fields specified in `source_field`
          * `{uuid}`: substitute with a UUID (v4); see the description for `uuid_seed` config var for information
            on how UUIDs are generated.  

## Sample ETL execution shell script
The commands below can be adapted and executed in a shell script such as `c3dc_etl.sh` for convenience.
```
# Creation of CCDI harmonized data file and transformation/mapping file involves the following steps:
# 1) If updating schema: update schema version var in ../../schema/.env and then either execute
#    ../../schema/schema_creator.py manually or uncomment schema script commands below
# 2) Update transformation/mapping version in ../../mapping_unpivoter/.env_mapping_unpivoter_study_identifier
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
    "chop_phs002517"
    "kumc_phs002529"
    "msk_phs002620"
    "msk_phs003111"
    "ohsu_phs002599"
    "pptc_phs001437"
    "ucsf_phs002430"
    "ucsf_phs002504"
    "usc_phs002518"
)

# enumerate each study to be harmonized; make sure version var has been updated if needed in
# ../../mapping_unpivoter/.env_mapping_unpivoter_study_identifier
for study in "${studies[@]}"
do
    echo "Harmonizing CCDI study \"$study\""
    # create transformation/mapping file; assuming starting dir is './etl/ccdi' where '.' is project root dir
    cd ../../mapping_unpivoter
    python mapping_unpivoter.py unpivot_transformation_mappings ".env_mapping_unpivoter_$study"
    cp mapping_unpivoter.log "mapping_unpivoter_$study.log"

    # run ETL script to create harmonized data file
    cd ../etl/ccdi
    python c3dc_etl.py ".env_$study"
    cp c3dc_etl.log "c3dc_etl_$study.log"
done
```
