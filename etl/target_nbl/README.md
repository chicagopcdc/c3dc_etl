# ETL and supporting scripts for C3DC Target Neuroblastoma (NBL) study

## Pre-requisites
Install Python (3.11 used at time of documentation) and add support for dependencies such as
[PETL](https://github.com/petl-developers/petl) by, for example,
[creating and activating a Conda environment](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file)
based on the [Conda environment file](https://github.com/chicagopcdc/c3dc_etl/blob/main/conda_environment.yml)

## ETL script
The c3dc_etl.py script ingests the source data in tabular (XLSX) format and transforms it into a harmonized (JSON)
data file.

### Execution steps
Create a local file named `.env` as per the [example](https://github.com/chicagopcdc/c3dc_etl/blob/main/etl/.env_example)
and execute the script without any arguments:
```
python c3dc_etl.py
```

### Configuration
Configuration has been divided into local and remote file instances. The local configuration file contains settings
specific to the local runtime environment such as local paths for input/output files. The remote configuration file
defines a `STUDY_CONFIGURATION` object that contains the harmonized data mappings for the corresponding source data
file and can be maintained in a version controlled repository so that changes can be tracked and audited as needed.
The remote configuration file will be loaded and then the resulting `STUDY_CONFIGURATION` object will be merged with
the matching local `STUDY_CONFIGURATION` object to configure the ETL script.
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
    * `name`: The unique name or identifier for this transformation.
    * `source_file_path`: The local path to the file containing the source data.
    * `output_file_path`: The local path to the file where the harmonized data will be saved.
    * `uuid_seed`: The optional seed to be passed to the random number generator used by the internal UUID creation
      function used to provision record identifiers such as `participant.participant_id`. UUIDs will be generated
      consistently across execution cycles if specified, otherwise they will be newly allocated with every script
      execution instance.
    * `active`: Whether this configuration object and the transformations specified within will be processed (true)
        or ignored (false).

#### Remote configuration (single `STUDY_CONFIGURATION` object that will be merged with matching local config object)
* `version`: The label identifying the version of this study configuration object.
* `transformations`: A list of objects, one per source file, containing configuration details needed to harmonize
    each source file.
    * `name`: The unique name or identifier for this transformation. Any 'local' transformation coniguration object
      with matching name will be merged with this one to form the final configuration object used by the ETL script.
    * `mappings`: The list of mapping objects that specify how source data fields will be harmonized for output.
    * `output_field`: The field, in `node_type.property_name` format, to which the harmonized data for this mapping
      will be saved. The node type and property name must correspond to node types and child properties specified in
      the [C3DC model](https://github.com/CBIIT/c3dc-model/blob/main/model-desc/c3dc-model.yml).
    * `source_field`: The source file field whose data value will be harmonized and saved to the `output_field`, e.g.
       `TARGET USI`, `Age at Diagnosis in Days`, `INSS Stage`, etc. The special value `[string_literal]` can be
       specified to indicate that the harmonized output value is derived from a static text value rather than a
       specific source file field.
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
    * `replacement_values`: A list of `replacement_value` objects that specify how 'old' source data values will be
      harmonized. Each `replacement_value` pair will be processed in sequence and the harmonized output value will be
      set to `new_value` if the source data matches the value or criteria specifed in `old_value`.
        * `old_value`: The old/existing value that will be mapped to the `new_value`. In addition to explicit values,
          the special characters `*` and `+` can be specified to indicate that the source data value will be
          replaced with `new_value` if the source data value is anything including null (`*`) or non-null (`+`).
          Partial wildcards such as `prefix*`, `*suffix`, and `*contains*` are not supported.
        * `new_value`: The value with which the source data should be replaced in the harmonized data output. In
          addition to explicit values, the following special values are also allowed:  
          `[uuid]`: substitute with a UUID (v4); see the description for `uuid_seed` config var for information on how
            UUIDs are generated.  
          `[field:{source field name}]`: substitute with the specified source field value for the current source
            record, for example `[field:TARGET USI]`.


## Mapping unpivoter utility script
The mapping unpivoter script can be used to transform the development team's internal shared document containing
harmonized data mapping definitions to the
[publicly available JSON transformation/mapping deliverable](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/transformations),
aka the 'remote' configuration file described above. The script can be used for convenience in lieu of editing and
maintaining the JSON transformation/mapping config file manually.

### Execution steps
1. Download the internal shared document as separate CSV files for each mapping sheet in the document. The
   'Microsoft Excel (.xlsx)' download option shortens and truncates the sheet/tab names, which makes it difficult for
   an automated script to detect the source-file => mapping entries. This may be supported in the future as more
   mappings are added, increasing the time and effort required to manually download a CSV file for each mapped sheet.
1. Create a local file named `.env_mapping_unpivoter` containing the configuration vars described below and execute the
   script without any arguments:
   ```
   python mapping_unpivoter.py
   ```

### Configuration
* `VERSION`: The value of the `version` config var to set for the `STUDY_CONFIGURATION` object contained in the
  resulting output file.
* `JSON_SCHEMA_URL`: The location of the JSON schema file that will be loaded and referenced to validate the
  `output_field` values contained in the source mapping files.
* `OUTPUT_FILE`: The local path to the file where the resulting JSON transformation/mapping will be saved.
* `TRANSFORMATION_MAPPINGS_FILES`: A string-ified list of objects specifying the source mapping file for a given
  transformation name. Each item will result in a `transformation` object in the output JSON config file having the
  `name` config var set to the value of `transformation_name` and the `mappings` config var set to the collection of
  mapping objects resulting from 'unpivoting' the contents of the file at the path specified in `mappings_file`.

#### Example `.env_mapping_unpivoter` configuration file:
```
VERSION='YYYYMMDD.N'
JSON_SCHEMA_URL='https://raw.githubusercontent.com/chicagopcdc/c3dc_etl/main/schema/schema.json'
OUTPUT_FILE='./transformations/phs000467.v22.p8.json'
TRANSFORMATION_MAPPINGS_FILES='[
    {
        "transformation_name": "TARGET_NBL_ClinicalData_Discovery_20220125.xlsx",
        "mappings_file": "/path/to/internal_tabular_mappings_file_for_target_nbl_cde_20170525_discovery_20220125.csv"
    },
    {
        "transformation_name": "TARGET_NBL_ClinicalData_Validation_20220125.xlsx",
        "mappings_file": "/path/to/internal_tabular_mappings_file_for_target_nbl_cde_20170525_validation_20220125.csv"
    }
]'
```