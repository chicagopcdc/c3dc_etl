# Tabular-to-JSON mapping unpivoter scripts

__CAUTION: be careful not to use this script to generate (overwrite) transformation/mapping JSON files that may be
directly authored/maintained__

The C3DC transformation/mapping rules are passed to the data harmonization scripts as a JSON file. The mappings
can be maintained in tabular format for ease of maintenance and curation, for example as an Excel (XLSX) file with
worksheets for each study, and then converted to JSON for use by the ETL scripts. This directory contains the python
script that performs the conversion, which can be conducted by following the steps below:
1. Create/update the [JSON schema version](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/README.md) of the
[C3DC model](https://github.com/CBIIT/c3dc-model/tree/main/model-desc).
1. Download and, if needed, customize the [C3DC Mappings.xlsx](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/C3DC%20Mappings.xlsx)
   file.
1. Create/update the `.env` environment file in the same directory; environment variables are described below and
   the sample [.env_example](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/.env_example)
   file can be used as a starting point and customized as needed. A file with a name other than `.env` can be
   specified as a command line argument as shown below.
1. Execute the command `python mapping_unpivoter.py unpivot_transformation_mappings "/path/to/config/file"`. The
   resulting output file can be passed to the relevant ETL script as the 'remote configuration' file specified in
   the `transformations_url` environment variable in the script's `.env` config file.

__NOTE__: `reference_file` records must be included in the final harmonized data output file for input files such
as the ETL script, transformation mapping file, and input source files. The reference file information that must
be present includes the relevant file's size and md5 hash. When the corresponding `reference_file` mappings are
present in the transformation mapping file, the ETL scripts can transform them into the necessary C3DC model
`reference_file` objects and inject them into the final harmonized data output file. The JSON transformation mapping
file created by the mapping unpivoter script is itself one of the reference files for which file size and md5 hash
must be calculated and included in the harmonized data output file. These self-referential properties are calculated
for the transformation mapping file with these values initially set to `0` (file size) and blank string (md5 hash).
The respective properties (`reference_file.file_size`, `reference.md5sum`) must then be set to the calculated
values. Setting the properties back to `0` and `""` respectively will then allow the previously calculated values to
be verified.

The mapping unpivoter script can be directed to set the `reference_file.file_size` and `reference_file.md5hash`
properties of the resulting transformation mapping file it creates by setting the
`AUTO_UPDATE_REFERENCE_FILE_MAPPINGS` configuration property to `True`. The script can also be directed to only
calculate and update the `reference_file` values within an existing transformation mapping file instead of
(re-)creating a new transformation mapping file by calling it with the appropriate argument:
`python mapping_unpivoter.py update_reference_file_mappings "/path/to/config/file"`.

## Environment variables
* `VERSION`: Required. String containing the version identifier, mapped to the output file's root-level `version`
  property. For example `20240604.1`.
* `JSON_SCHEMA_URL`: Required. String containing the URL of the JSON-formatted C3DC model schema. Can also be a local
  file path. For example `https://raw.githubusercontent.com/chicagopcdc/c3dc_etl/main/schema/schema.json` or
  `/path/to/schema.json`.
* `OUTPUT_FILE`: Required. String containing the JSON transformation mapping file output path. Corresponds to the
  'remote configuration' file used by the ETL script and specified in the `transformations_url` environment variable
  of the ETL script's `.env` config file. For example `/path/to/transformation_mapping.json`.
* `ETL_SCRIPT_FILE`: Optional. String containing the path to the ETL script that will perform the harmonization for
  the transformation mapping file. Required if updating reference file size and md5 hash e.g. when the
  `AUTO_UPDATE_REFERENCE_FILE_MAPPINGS` config var is set to `True` or the `mapping_unpivoter.py` script is invoked
  with the `update_reference_file_mappings` argument. For example `/path/to/c3dc_etl.py`.
* `AUTO_UPDATE_REFERENCE_FILE_MAPPINGS`: Optional. Boolean string (`True` or `False`, default `False`) that
  indicates whether the `mapping_unpivoter.py` script will automatically find and update the self-referential
  `reference_file.file_size` and `reference_file.md5sum` properties of the transformation mapping output file
  (`OUTPUT_FILE`). In addition, the same properties for the following reference files will be updated if specified
  in the configuraiton:
  * ETL script (`ETL_SCRIPT_FILE`)
  * JSON schema (`JSON_SCHEMA_URL`)
  * Excel (XLSX) mappings file (`mappings_file` below)
  * Source data file (`source_data_file` below)
* `TRANSFORMATION_MAPPINGS_FILES`: Required. A string-ified list of objects that contain transformation name, mappings
  file path, mappings file sheet, and (optional) source data file path in the following properties:
  * `transformation_name`: Required. String containing the name of the transformation; mapped to the `name`
    property of the resulting `transformation` object captured in the output file. For example `phs002790`.
  * `mappings_file`: Required. Path to the source Excel (XLSX) document containing the mappings which will be
    converted to JSON. For example `/path/to/C3DC Mappings.xlsx`.
  * `mappings_file_sheet`: Required. String containing the name of the worksheet, typically the study id, containing
    the mappings specific to this transformation. For historical reasons __only the first 31 characters of the sheet
    name will be used__ so this property and the sheet names in the source XLSX mappings file should be set as needed
    to match. For example `phs002790`.
  * `source_data_file`: Optional. String containing the path to the source data file that will be harmonized. For
    example `/path/to/source_data.xlsx`. Can be omitted for MCI (`phs002790`) because there is custom logic in the
    MCI ETL script that adds reference file mappings for each of the individual source data files harmonized.

## Excel (XLSX) mapping file columns
Unknown columns will be ignored. See the remote configuration section in the README file for each harmonization study
group (CCDI, MCI, Target NBL) for details on the transformed output JSON mapping file.
* `Source Variable Name`: Required. Corresponds to the `source_field` property of the transformed output JSON mapping
  file. Unquoted string containing the name of the field in the input source data such as the XLSX column name
  (CCDI template, TARGET) or JSON attribute name (MCI) that will be harmonized. For example `RACE`, `DM_CRACE`, and
  `participant.race` to map the `participant.race` property for TARGET, MCI, and CCDI respectively. The special value
  `[string_literal]` can be specified to indicate that the harmonized output value is derived from a static text value
  or function rather than a specific source file field. Multiple fields can be specified as a comma-separated list for
  use by 'macro-like' functions such as `{sum}` (see `Target Permissible Values Term` below).
* `Source Permissible Values Term`: Optional. Corresponds to the `replacement_values.old_value` property of the
  transformed output JSON mapping file. If not specified (blank/empty), the harmonized value of this field will be
  set to the unmodified value retrieved from the input source data. If specified, must be a string enclosed in double
  quotes (`"string"`) for which matching source values will be harmonized to the value of the corresponding
  `Target Permissible Values Term` property. In other words, the value of `Source Permissible Values Term` is the
  'old' value to be replaced by the 'new' value in `Target Permissible Values Term`. For example
  `"old value to be matched and replaced"`. In addition to explicit values, the special values `"*"` and `"+"` can
  be specified to indicate that the source data value will be replaced with the value of
  `Target Permissible Values Term` if the source data value is any value including blank/empty (`"*"`) or non-null
  (`"+"`). Note that partial wildcards such as `"prefix*"`, `"*suffix"`, and `"*contains*"` are __NOT__ supported.
* `Source Permissible Values Code`: Currently unused; reserved for potential use in the future.
* `Target Variable Name`: Required. Corresponds to the `output_field` property of the transformed output JSON mapping
  file. Unquoted string containing the field name, in `node_type.property_name` format, to which the harmonized data
  for this mapping will be saved. The node type and property name must correspond to node types and child properties
  specified in the [C3DC model](https://github.com/CBIIT/c3dc-model/blob/main/model-desc/c3dc-model.yml). For example
  `participant.race`, `diagnosis.anatomic_site`, etc.
* `Target Permissible Values Term`: Optional. Corresponds to the `replacement_values.new_value` property of the
  transformed output JSON mapping file. A string enclosed in double quotes (`"string"`) containing the harmonized
  value with which to replace the matching `Source Permissible Values Term` property. If the output field specified in
  `Target Variable Name` has type `list`
  (such as [participant.race](https://github.com/CBIIT/c3dc-model/blob/1.2.0/model-desc/c3dc-model-props.yml#L1992))
  then the value must be specified as a group of one or more strings individually enclosed in double quotes and
  separated by commas (`,`) with the entire group enclosed in square brackets (`["Not Reported"]` or
  `["White", "Black or African American"]`). If the `Source Permissible Values Term` property is not specified then
  this property will be ignored and the harmonized value of this field will be set to the unmodified value retrieved
  from the input source data. In addition to explicit values, the following special values are also allowed:
  * `{field:source field name}`: Replace with the specified source field value for the current source record, for
    example `{field:TARGET USI}`.
  * `{find_enum_value}`: **TARGET and MCI only**. Replace with the enum (aka permissible list) value found using the
    source data value of the `Source Variable Name` field as the key. For permissible list values containing the
    separator ` : ` the search will attempt to match on the code/prefix first and then the entire enum value. For
    example, for `diagnosis.anatomic_site`, the source value `C22.0` would match `C22.0 : Liver`. If there are
    multiple permissible list values with the same code/prefix then the **last** matching entry would be used as the
    replacement value.
  * `{sum}`: Replace with the sum of the values for the source fields specified in `Source Variable Name`.
  * `{sum_abs_first}`: **MCI only**. Replace with the sum of the values (absolute value of first addend) for the source
    fields specified in `Source Variable Name`.
  * `{uuid}`: Replace with a unique id (UUID v4); the generated uuid values can be made deterministically reproducible
    by specifying a static seed value in the ETL script configuration.
* `Target Permissible Values Code`: Currently unused; reserved for potential use in the future.
* `Type Group Index`: Optional. Corresponds to the `type_group_index` property of the transformed output JSON mapping
  file. An integer identifier to be used if this field's mapping will apply to multiple records. If
  more than one type group index is present in the mappings for a given output field then multiple records of the
  target output node type will be created for each record present in the input source data. This is typically used
  for reference files, since multiple reference file entries (programmatic source code, transformation, schema,
  input source data file) must be present for a single source record. `Type Group Index` does not need to be specified
  for single record mappings, which are for fields associated with a single output record per source record such as
  `participant`. Every record in the source file that contains valid data will correspond to a single `participant`
  object in the harmonized data file and therefore field mappings for the `participant` object do not need to specify
  a value for the `Type Group Index` column. Multi-record mappings are needed for `reference_file` output objects to
  account for the reference files involved in the ETL process such as the ETL script, JSON validation schema, JSON
  transformation/mapping specification file, and input source files. As a result there are multiple collections of
  field mappings for each `reference_file` object needed and a distinct `Type Group Index` value associated with each
  mapping group. The wildcard value `*` (**NOT** enclosed in quotes) can be used to indicate mappings that apply to
  all records of that type such as `reference_file.reference_file_id`, which specifies that all `reference_file`
  records will have their `reference_file_id` values set to a programmatically-generated UUID.
* `Default Value If Null/Blank`: Optional. Corresponds to the `default_value` property of the transformed output JSON
  mapping file. The value to which the harmonized output field will be set when the input source data value of this
  field (`Source Variable Name`) is blank/null. String values (`"string"`) must be enclosed in double quotes while
  integers such as `-999` must be left unquoted. If the output field specified in
  `Target Variable Name` has type `list` (such as
  [participant.race](https://github.com/CBIIT/c3dc-model/blob/1.2.0/model-desc/c3dc-model-props.yml#L1992)) then the
  value must be specified as a group of one or more strings individually enclosed in double quotes and separated by
  commas (`,`) with the entire group enclosed in square brackets (`["Not Reported"]` or
  `["White", "Black or African American"]`).
* `Replacement Values`: Optional. A collection of `"source value" => "replacement value"` pairs that can be used to
  apply on-demand corrections to invalid source data values during harmonization. For example for the CCDI UCSF
  `phs002430` study: `{"C80 : UNKNOWN PRIMARY SITE": "C80.9 : Unknown primary site"}`. Equivalent functionality can
  be achieved by entering multiple mapping rows with one row per replacement value and all field values the same
  except `Source Permissible Values Term` and `Target Permissible Values Term`. However using the `Replacement Values`
  column makes it more obvious for mapping maintainers that corrective replacement is the intent as opposed to
  explicit mapping of constrained/permissible source values such as the D4CG-maintained mappings for TARGET and MCI
  fields such as `participant.race` and `participant.ethnicity` where each value in the source data is mapped to an
  explicit harmonized output value.
