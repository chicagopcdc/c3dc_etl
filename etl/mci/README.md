# ETL and supporting scripts for Molecular Characterization Initiative (MCI) study

## Pre-requisites
Install Python (3.11 used at time of documentation) and add support for dependencies such as
[PETL](https://github.com/petl-developers/petl) by, for example,
[creating and activating a Conda environment](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file)
based on the [Conda environment file](https://github.com/chicagopcdc/c3dc_etl/blob/main/conda_environment.yml)

## Script to collate and pivot study data in JSON files
The MCI source data is provided as individual JSON files per subject. To make it easier to browse and inspect the
data, the data pivoter script gathers and pivots the source data into a single Excel (XLSX) workbook containing
a worksheet for each 'form', with properties mapped to columns, and one row per subject. A comprehensive sheet
(`ALL_FORMS`) is also present that contains all subjects and properties found across all source files.

### Data pivoter script execution
Call the script with two arguments, the first the path to the file or a directory containing the JSON data to be
pivoted and the second the destination file where the pivoted data should be saved. If a directory is passed as
an input then data from all JSON files within that directory and sub-directories will be pivoted. If the destination
file already exists a copy will be saved in the same directory, e.g. `filename.xlsx` => `filename_last.xlsx`.

Example command:
```
python data_pivoter.py /path/to/cog_mci_ingested_json_files/ cog_mci_ingested_pivoted_data.xlsx
```

## Data harmonization process and input dependencies
The c3dc_etl.py script ingests the source data JSON files, collating and transforming them into a single harmonized
(JSON) data file by applying the mapping rules in the [JSON transformation/mapping file](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/mci/transformations)
and then performs validation against the [JSON schema version](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/schema.json)
of the [C3DC model](https://github.com/CBIIT/c3dc-model/tree/main/model-desc).

Note: the transformation mapping JSON file contains `reference_file` entries for harmonization inputs such as
the ETL script, JSON schema, input source files and the transformation mapping file itself. These `reference_file`
mapping entries are transformed into `reference_file` records that are injected into the harmonized data output
file by the ETL script. The ETL script can add the necessary `reference_file` mapping records to the transformation
mapping file to eliminate the need to manually maintain the `reference_file` entries for each input source file in
the transformation mapping file. The ETL script will catalog the input source files as they are harmonized and, if
there are no `reference_file` entries in the transformation mapping file having `file_category` set to
`input source data`, it will add `reference_file` mapping entries to a copy of the transformation mapping file. The
ETL script can then be executed again using the amended transformation mapping file, which will result in the
`reference_file` input source file entries being written to the harmonized data output file.

Thus the execution of the MCI ETL will typically be a two-step process, with the first step creating an amended
transformation mapping file containing the `input source data` reference file entries (the harmonized data output
file will still be created but should be discarded) and the second step incorporating the newly amended
transformation/mapping to create the final harmonized data output file.

### C3DC model JSON schema
The data harmonization script uses the [C3DC JSON schema file](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/schema.json)
for reference and validation, which is converted from the [C3DC model's](https://github.com/CBIIT/c3dc-model/tree/main/model-desc)
source YAML files. See the [C3DC JSON schema README file](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/README.md)
for more information.

### Mapping unpivoter utility script
The [mapping unpivoter script](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter) can be used to
transform the Excel (XLSX) document containing harmonized data mapping definitions to the
[publicly available JSON transformation/mapping deliverable](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/ccdi/transformations),
aka the 'remote' JSON configuration file described below referenced in the `transformations_url` property. It's
recommended to use the script in lieu of editing and maintaining the JSON transformation/mapping config file manually.
See the [README file](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/README.md) for configuration
and execution details.

## Mapping rules for `treatment` and `treatment_response` records
The `treatment` and `treatment_response` mapping rules are formatted and processed differently than the mappings for
other objects like `participant`, `diagnosis`, `survival`, etc, that are contained in the Excel (XLSX) document
referenced above and described in the 
[README file for the mapping unpivoter scripts](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/README.md#excel-xlsx-mapping-file-columns).
The `treatment` and `treatment_response` mapping rules are specified in a separate tab/sheet with each row containing
a mapping rule which, when matched, will result in a distinct `treatment` or `treatment_response` record being
added to the harmonized output file for the corresponding subject. Each mapping rule specifies the source record
field(s) to check and the value(s) of the source record field(s) that will be considered a match. The remaining
fields of the mapping rule specify the values of the resulting `treatment` or `treatment_response` record that will
be added to the harmonized output file.

### Treatment mapping rule columns
* `Source Variable Name`: Required. String specifying the name of the source record field to be evaluated for a match.
  Note that the field name can be specified as `form.field_name`, for example `ON_STUDY_DX_CNS.TUM_RES_EXT_TP`, to
  uniquely identify fields that may exist in multiple forms.
* `Source Variable Value`: Required. String specifying the value of the source record field specified above in
  `Source Variable Name` which will be evaluated for a match resulting in a `treatment` record for the source record
  subject being added to the harmonized output. A source record will be considered to be a match for the mapping rule
  if the value of the field specified in `Source Variable Name` is equal to the value specified in
  `Source Variable Value`.
* `treatment.treatment_id`: Required. String specifying the value of the harmonized `treatment` record's `treatment_id`
  field. The 'macro-like' function `{uuid}` can be used to indicate that the value of the `treatment_id` field should
  be set to a UUID using the same mechanism described below in the [Configuration](#configuration) section.
* `treatment.treatment_agent`: Required. String specifying the value of the harmonized `treatment` record's
  `treatment_agent` field.
* `treatment.treatment_type`: Required. String specifying the value of the harmonized `treatment` record's
  `treatment_type` field.
* `treatment.age_at_treatment_start`: Required. Integer specifying the value of the harmonized `treatment` record's
  `age_at_treatment_start` field. The value `-999` can be used as a default value to indicate Not Available, Not
  Reported, Unknown, etc. The 'macro-like' function `{sum_abs_first}` can also be specified to indicate that the value
  of the `age_at_treatment_start` field should be set to the sum of two source record fields. For example
  `{sum_abs_first(DM_BRTHDAT, PT_SURGICAL_RESEC_DT)}` would result in the `age_at_treatment_start` field being set to
  the sum of the absolute value of the `DM_BRTHDAT` field and the as-is value of the `PT_SURGICAL_RESEC_DT` field.
* `treatment.age_at_treatment_end`: Required. Integer specifying the value of the harmonized `treatment` record's
  `age_at_treatment_end` field. The value `-999` can be used as a default value to indicate Not Available, Not
  Reported, Unknown, etc. The 'macro-like' function `{sum_abs_first}` can also be specified to indicate that the value
  of the `age_at_treatment_end` field should be set to the sum of two source record fields. For example
  `{sum_abs_first(DM_BRTHDAT, PT_SURGICAL_RESEC_DT)}` would result in the `age_at_treatment_end` field being set to
  the sum of the absolute value of the `DM_BRTHDAT` field and the as-is value of the `PT_SURGICAL_RESEC_DT` field.

### Example mapping rules for `treatment` records
For the example mapping rules below and a source record having field `FSTLNTXINIDXADMCAT_A1` set to `checked`, a
`treatment` record would be added for the corresponding subject with `treatment_id` set to a randomly generated UUID,
`treatment_agent` set to `Not Reported`, `treatment_type` set to `Pharmacotherapy` and both `age_at_treatment_start`
and `age_at_treatment_end` set to -999. Each mapping rule row is evaluated independently so if the source record
also matched each of the remaining mapping rules (`ON_STUDY_DX_CNS.TUM_RES_EXT_TP` = `Gross Total Resection`,
`SURG_RESECTION_OSTEOSARCOMA.TUM_RES_EXT_TP` = `Gross Total Resection`, and `AGT_ADM_NM_A01` = `checked`) there would
be a total of 4 `treatment` records added to the harmonized output for the source record subject having the values
specified in each mapping rule's `treatment.*` fields. 

Source Variable Name | Source Variable Value | treatment.treatment_id | treatment.treatment_agent | treatment.treatment_type | treatment.age_at_treatment_start | treatment.age_at_treatment_end
--- | --- | --- | --- | --- | --- | ---
FSTLNTXINIDXADMCAT_A1 | checked | {uuid} | Not Reported | Pharmacotherapy | -999 | -999
ON_STUDY_DX_CNS.TUM_RES_EXT_TP | Gross Total Resection | {uuid} | Not Reported | Surgical Procedure | {sum_abs_first(DM_BRTHDAT, PT_SURGICAL_RESEC_DT)} | -999
SURG_RESECTION_OSTEOSARCOMA.TUM_RES_EXT_TP | Gross Total Resection | {uuid} | Not Reported | Surgical Procedure | {sum_abs_first(DM_BRTHDAT, PT_SURGICAL_RESEC_DT)} | -999
AGT_ADM_NM_A01 | checked | {uuid} | Isotretinoin | Pharmacotherapy | -999 | -999  

&nbsp;

### Treatment response mapping columns
* `Source Variable Name 1`: Required. String specifying the name of the first source record field to be checked for
  a match. Note that the field name can be specified as `form.field_name`, for example
  `FOLLOW_UP.COMP_RESP_CONF_IND_3`, to uniquely identify a field that may exist in multiple forms.
* `Source Variable Value 1`: Required. String specifying the value of the first source record field specified above in
  `Source Variable Name 1` which will be evaluated for a match resulting in a `treatment` record for the source record
  subject being added to the harmonized output.
* `Source Variable Name 2`: Required. String specifying the name of the second source record field to be checked for
  a match. As with `Source Variable Name 1` the field name can be specified as `form.field_name`, for example
  `FOLLOW_UP.DZ_EXM_REP_IND_2`, to uniquely identify a field that may exist in multiple forms.
* `Source Variable Value 2`: Required. String specifying the value of the first source record field specified above in
  `Source Variable Name 2` which will be evaluated for a match resulting in a `treatment` record for the source record
  subject being added to the harmonized output. A source record will be considered to be a match for the mapping rule
  if the value of the field specified in `Source Variable Name 1` is equal to the value specified in
  `Source Variable Value 1` __AND ALSO__ the value of the field specified in `Source Variable Name 2` is equal to the
  value specified in `Source Variable Value 2`.
* `treatment.treatment_response_id`: Required. String specifying the value of the harmonized `treatment_response`
  record's `treatment_response_id` field. The 'macro-like' function `{uuid}` can be used to indicate that the value
  of the `treatment_response_id` field should be set to a UUID using the same mechanism described below in the
  [Configuration](#configuration) section.
* `treatment_response.response`: Required. String specifying the value of the harmonized `treatment_response` record's
  `response` field.
* `treatment_response.age_at_response`: Required. Integer specifying the value of the harmonized `treatment_response`
  record's `age_at_response` field. The value `-999` can be used as a default value to indicate Not Available, Not
  Reported, Unknown, etc. The 'macro-like' function `{sum_abs_first}` can also be specified to indicate that the
  value of the `age_at_treatment_start` field should be set to the sum of two source record fields. For example
  `{sum_abs_first(DM_BRTHDAT, PT_SURGICAL_RESEC_DT)}` would result in the `age_at_response` field being set to the
  sum of the absolute value of the `DM_BRTHDAT` field and the as-is value of the `PT_SURGICAL_RESEC_DT` field.
* `treatment_response.response_category`: Required. String specifying the value of the harmonized `treatment_response`
  record's `response_category` field.
  be set to a UUID using the same mechanism described above in the used for the study configuration section.
* `treatment_response.response_system`: Required. String specifying the value of the harmonized `treatment_response`
  record's `response_system` field.

### Example mapping rules for `treatment_response` records
For the example mapping rules below and a source record having field `COMP_RESP_CONF_IND_3` set to `Yes` and
`DZ_EXM_REP_IND_2` set to `Yes`, a `treatment_response` record would be added for the corresponding subject with
`treatment_response_id` set to a randomly generated UUID, `response` set to `Complete Remission`, `age_at_response`
set to `-999` and both `response_category` and `response_system` set to `Not Reported`. Some of the mapping
rules can in practice be evaluated using a single field, for example `COMP_RESP_CONF_IND_3` = `Unknown` results in a
`treatment_response` record having `response` set to `Unknown` regardless of the value of `DZ_EXM_REP_IND_2`. However
the mapping rules were implemented to always match sources records by evaluating two fields to simplify the
harmonization process.

Source Variable Name 1 | Source Variable Value 1 | Source Variable Name 2 | Source Variable Value 2 | treatment_response.treatment_response_id | treatment_response.response | treatment_response.age_at_response | treatment_response.response_category | treatment_response.response_system
--- | --- | --- | --- | --- | --- | --- | --- | ---
COMP_RESP_CONF_IND_3 | Yes | DZ_EXM_REP_IND_2 | Yes | {uuid} | Complete Remission | -999 | Not Reported | Not Reported
COMP_RESP_CONF_IND_3 | Yes | DZ_EXM_REP_IND_2 | No | {uuid} | Complete Remission | -999 | Not Reported | Not Reported
COMP_RESP_CONF_IND_3 | Yes | DZ_EXM_REP_IND_2 |  | {uuid} | Complete Remission | -999 | Not Reported | Not Reported
COMP_RESP_CONF_IND_3 | No | DZ_EXM_REP_IND_2 | Yes | {uuid} | Unknown | -999 | Not Reported | Not Reported
COMP_RESP_CONF_IND_3 | No | DZ_EXM_REP_IND_2 | No | {uuid} | Not Done | -999 | Not Reported | Not Reported
COMP_RESP_CONF_IND_3 | No | DZ_EXM_REP_IND_2 |  | {uuid} | Unknown | -999 | Not Reported | Not Reported
COMP_RESP_CONF_IND_3 | Unknown | DZ_EXM_REP_IND_2 | Yes | {uuid} | Unknown | -999 | Not Reported | Not Reported
COMP_RESP_CONF_IND_3 | Unknown | DZ_EXM_REP_IND_2 | No | {uuid} | Unknown | -999 | Not Reported | Not Reported
COMP_RESP_CONF_IND_3 | Unknown | DZ_EXM_REP_IND_2 |  | {uuid} | Unknown | -999 | Not Reported | Not Reported

## ETL script
The c3dc_etl.py script ingests the source data in tabular (XLSX) format and transforms it into a harmonized (JSON)
data file.

### Execution steps
1. Create/update the [JSON schema version](https://github.com/chicagopcdc/c3dc_etl/tree/main/schema)
   of the [C3DC model](https://github.com/CBIIT/c3dc-model/tree/main/model-desc) if not using the publicly available
  [version](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/schema.json) published in GitHub by the D4CG team.
1. Create/update the [JSON transformation/mapping file](https://github.com/chicagopcdc/c3dc_etl/tree/main/mapping_unpivoter)
   for the study if not using the publicly available
   [version](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/mci/transformations)
   published in GitHub by the D4CG team.
1. Create a local file named `.env` (see below for configuration details) as per the
   [example](https://github.com/chicagopcdc/c3dc_etl/blob/main/etl/mci/.env_example) and execute the script
   without any arguments:
   ```
   python c3dc_etl.py
   ```
   A config file with a name other than `.env` can be specified as a command line argument:
   ```
   python c3dc_etl.py "/path/to/config/file"
   ```

### Configuration
Configuration has been divided into local and remote file instances. The local configuration file is a .env file that
contains settings specific to the local runtime environment such as local paths for input/output files while the
remote configuration file is a JSON file containing the data transformation/mapping rules and is environment agnostic.
This separation allows the remote configuration files to be maintained in a version controlled repository such as
[GitHub](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/mci/transformations) so that changes can be tracked
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
The sample [.env_example](https://github.com/chicagopcdc/c3dc_etl/blob/main/etl/mci/.env_example) file can be used
as a starting point and customized as needed.
* `JSON_SCHEMA_URL`: Required. String containing the location of the JSON schema file that will validate the
  harmonized data file created by the ETL script. Can also be a local file path. For example
  `https://raw.githubusercontent.com/chicagopcdc/c3dc_etl/main/schema/schema.json` or `/path/to/schema.json`
* `STUDY_CONFIGURATIONS`: Required. A string-ified list of objects, with one configuration object per study.
  * `study`: Required. The unique name or identifier for this study configuration object. This value must be unique
    within the list of study configuration objects. For example `phs002790`.
  * `active`: Optional. Boolean string (`True` or `False`, default `False`) that indicates whether this configuration
    object and the transformations specified within will be processed (`True`) or ignored (`False`).
  * `transformations_url`: Required. String specifying the location of the file containing the transformations
    configuration to be merged with the local transformation specified in the `transformations` config variable.
    Can also be a local file path. For example
    `https://raw.githubusercontent.com/chicagopcdc/c3dc_etl/main/etl/mci/transformations/phs002790.json` or
    `/path/to/phs002790.json`.
  * `transformations`: Required. A string-ified list of objects, one per source data file, containing configuration
    details needed to harmonize each source data file. Each transformation will be matched and merged with an object
    in the remote config by `name`.
    * `name`: Required. String containing the unique name or identifier for this transformation. This value must be
      unique within the list of transformation and must match the name of the transformation specified in the
      transformation/mapping file located at `transformations_url`. For a transformation/mapping file created using
      the mapping unpivoter script, the name value would correspond to the `transformation_name` of the
      [environment variable](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/README.md#environment-variables).
      For example `phs002790`.
    * `source_file_path`: Required. String specifying the location of the file containing the source data. Can be a
      local path or AWS S3 URL. For example `/path/to/source/file` or `s3://bucket/path/to/source/file`.
    * `source_file_manifest_path`: Optional. String specifying the location of the Excel (XLSX) source data manifest
      file containing metadata about each source file such as globally unique identifier (guid), size, MD5 hash, and
      URL. Can be a local path or AWS S3 URL. For example `/path/to/source/file/manifest.xlsx` or
      `s3://bucket/path/to/source/file/manifest.xlsx`.
    * `source_file_manifest_sheet`: Optional. String specifying the name of the source data manifest file worksheet
      containing the metadata rows. Defaults to `clinical_measure_file` if not specified.
    * `treatment_mappings_path`: Optional. String specifying the location of the Excel (XLSX) document containing
      the mapping rules that will be used to find and derive `treatment` records. Can be a local path or AWS S3 URL.
      For example `/path/to/source/file/manifest.xlsx` or `s3://bucket/path/to/source/file/manifest.xlsx`.
    * `treatment_mappings_sheet`: Optional. String specifying the name of the treatment mappings file worksheet
      containing the mapping rows. Defaults to `phs002790_treatment` if not specified.
    * `treatment_response_mappings_path`: Optional. String specifying the location of the Excel (XLSX) document
      containing the mapping rules that will be used to find and derive `treatment_response` records. Can be a local
      path or AWS S3 URL. For example `/path/to/source/file/manifest.xlsx` or
      `s3://bucket/path/to/source/file/manifest.xlsx`.
    * `treatment_response_mappings_sheet`: Optional. String specifying the name of the treatment mappings file
      worksheet containing the mapping rows. Defaults to `phs002790_treatment_response` if not specified.
    * `output_file_path`: Required. String specifying the location of the file where the harmonized data will be
      saved. Can be a local path or AWS S3 URL. For example `/path/to/harmonized/data/file` or
      `s3://bucket/path/to/harmonized/data/file`.
    * `uuid_seed`: Optional. String containing the seed value to be passed to the random number generator used by
      the internal UUID creation function called to provision record identifiers such as
      `reference_file.reference_file_id`. UUIDs will be deterministically reproducible across execution cycles if
      specified, otherwise they will be newly allocated with every script execution instance. For example `phs002790`.
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
            record, for example `"{field:upi}"`.
          * `"{find_enum_value}"`: Replace with the enum (aka permissible value list) entry found using the source
            field value as the key. For permissible list values containing the separator ` : ` the search will attempt
            to match on the code/prefix first and then the entire enum value. For example, for
            `diagnosis.anatomic_site`, the source value `C22.0` would match `C22.0 : Liver`. If there are multiple
            permissible list values with the same code/prefix then the **last** matching entry would be used as the
            replacement value.
          * `"{race}"`: Replace with the calculated race value based on the value(s) specified in the source
            field(s). Historically MCI source data contained both race (`DM_CRACE`) and ethnicity (`DM_ETHNIC`)
            values, for example `White` and `Hispanic or Latino`. This macro can be used to determine the race value
            in accordance with the
            [OMB standard to maintain, collect and present race and ethnicity as of 2024](https://www.federalregister.gov/documents/2024/03/29/2024-06469/revisions-to-ombs-statistical-policy-directive-no-15-standards-for-maintaining-collecting-and)
            whether the source data contains both race and ethnicity or only race.
          * `"{sum}"`: Replace with the sum of the values for the source fields specified in `source_field`.
          * `"{sum_abs_first}"`: Replace with the sum of the values (abs value of first addend) for the source
            fields specified in `source_field`.
          * `"{uuid}"`: Replace with a UUID (v4); see the description for `uuid_seed` config var for information
            on how UUIDs are generated.

## Sample ETL execution shell script
The commands below can be adapted and executed in a shell script such as `c3dc_etl.sh` for convenience to consolidate
all of the data harmonization steps for all studies into a single executable script.
```
# Creation of MCI harmonized data file and transformation/mapping file involves the following steps:
# 1) If updating schema: update schema version var in ../../schema/.env and then either execute
#    ../../schema/schema_creator.py manually or uncomment schema script commands below
# 2) Update transformation/mapping version in ../../mapping_unpivoter/.env_mapping_unpivoter_phs002790
# 3) Create base transformation/mapping file without reference file entries for input source data files
# 4) Run ETL to add reference file entries for input source data files e.g. in ./COG_MCI_json_ingest2.
#    Harmonized data file created in this step should be ignored/discarded.
# 5) Update reference file size and md5sum entries for transformation/mapping file (self/referential,
#    from 0 and '' initial values respectively)
# 6) Run ETL again to create final harmonized data file with updated reference file entries 

# activate conda env first with e.g. 'conda activate c3dc_etl'; check conda_environment.yml for package dependencies

# exit on error
set -e

# if updating schema, uncomment the following after updating the version var in the ../../schema.env file:
# cd ../../schema
# python schema_creator.py

# create log dirs if needed
mkdir -p logs
mkdir -p ../../mapping_unpivoter/logs

# update version var in ../../mapping_unpivoter/.env_mapping_unpivoter_phs002790 and then create base
# transformation/mapping file; assuming starting dir is './etl/mci' where '.' is project root dir
cd ../../mapping_unpivoter
python mapping_unpivoter.py unpivot_transformation_mappings .envs/.env_mapping_unpivoter_phs002790
cp mapping_unpivoter.log logs/mapping_unpivoter_phs002790_1.log

# run ETL script to create transformation/mapping file containing ref file entries for input source data files
cd ../etl/mci
python c3dc_etl.py

# save copy of current transformation/mapping file and overwrite with updated version created by ETL script
cp transformations/phs002790.json transformations/phs002790.json.bak
mv transformations/phs002790.ref_files.json transformations/phs002790.json

# update transformation/mapping file's reference file size and md5sum mappings
cd ../../mapping_unpivoter
python mapping_unpivoter.py update_reference_file_mappings .envs/.env_mapping_unpivoter_phs002790
cp mapping_unpivoter.log logs/mapping_unpivoter_phs002790_2.log

# run ETL script again to create final harmonized data file
cd ../etl/mci
python c3dc_etl.py
```
