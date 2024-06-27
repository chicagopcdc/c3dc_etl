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
   specified as a command line argument:
1. Execute the command `python mapping_unpivoter.py unpivot_transformation_mappings "/path/to/config/file"`

__NOTE__: `reference_file` records must be included in the final harmonized data output file for input files such
as the ETL script, transformation mapping file, and input source files. The reference file information that must
be present includes the relevant file's size and md5 hash. When the corresponding `reference_file` mappings are
present in the transformation mapping file, the ETL scripts can transform them into the necessary C3DC model
`reference_file` objects and inject them into the final harmonized data output file. The JSON transformation mapping
file created by the mapping unpivoter script is itself one of the reference files for which file size and md5 hash
must be calculated and included in the harmonized data output file. These self-referential properties are calculated
for the transformation mapping file with these values initially set to 0 (file size) and blank string (md5 hash).
The respective properties (`reference_file.file_size`, `reference.md5sum`) must then be set to the calculated
values. Setting the properties back to 0 and '' respectively should then allow the previously calculated values to
be verified.

The mapping unpivoter script can be directed to set the `reference_file.file_size` and `reference_file.md5hash`
properties of the resulting transformation mapping file it creates by setting the
`AUTO_UPDATE_REFERENCE_FILE_MAPPINGS` configuration property to `True`. The script can also
be be directed to only calculate and set these values for the transformation mapping file by calling it with
the appropriate argument:
`python mapping_unpivoter.py update_reference_file_mappings "/path/to/config/file"`.
When the `update_reference_file_mappings` argument is passed, the script will not (re-)create
the output transformation mapping file, it will only calculate the file size and md5 has for the existing file
specified in the `OUTPUT_FILE` configuration variable and set the appropriate `reference_file` properties matching
the file.

## Environment variables:
* `VERSION`: The version identifier, mapped to the `version` root-level property
* `JSON_SCHEMA_URL`: The URL of the JSON-formatted C3DC model schema
* `OUTPUT_FILE`: JSON transformation mapping file output path
* `ETL_SCRIPT_FILE`: Path to ETL script associated with transformation mapping file; used if updating reference
  file size and md5 hash e.g. when the `AUTO_UPDATE_REFERENCE_FILE_MAPPINGS` config var is set to `True` or the
  script is invoked with the `update_reference_file_mappings` argument.
* `AUTO_UPDATE_REFERENCE_FILE_MAPPINGS`: A boolean string that indicates whether the
  script should automatically find and update the self-referential `reference_file.file_size` and
  `reference_file.md5sum` properties of the output file, ETL script (if specified) and JSON schema. If enabled,
  the script will calculate the size and md5 checksum (as hex string)of the transformation mapping file immediately
  after creation. It will then search the file for the relevant `reference_file` record's `file_size` and `md5sum`
  properties and set them accordingly.
* `TRANSFORMATION_MAPPINGS_FILES`: A string-ified list of objects that contain transformation name, mappings
  file path, and (optional) source data file path in the following properties:
  * `transformation_name`: The name of the transformation; mapped to the `name` property of the resulting
    `transformation` object captured in the output file.
  * `mappings_file`: Path to the source tabular document (e.g. XLSX or CSV) which will be converted to JSON. For a
    CSV source the entire document will be converted to JSON and the `mappings_file_sheet` property will be ignored.
    For an XLSX source the name of the worksheet containing the mappings for this transformation must be specified.
  * `mappings_file_sheet`: The name of the worksheet containing the mappings specific to this transformation. This
    property is ignored for non-XLSX sources e.g. CSV. For historical reasons __only the first 31 characters of the
    sheet name will be used__ so this property and the sheet names in the source XLSX mappings file should be set
    as needed to match.
  * `source_data_file`: Optional path to the source data file for the transformation/mapping. If populated the
    mapping unpivoter script will include this file when updating the reference file size and md5 sum mappings
    properties.

## Tabular mapping file columns:
Unknown columns will be ignored. See the remote configuration/`STUDY_CONFIGURATION` section in the README
for each harmonization group (CCDI, MCI, Target NBL) for details on the transformed output JSON mapping file.
* `Source Variable Name`: The name of the field in the input source data such as the XLSX column name (CCDI template,
  Target NBL) or JSON attribute name (MCI) that will be harmonized. Corresponds to the `source_field` property of the
  transformed output JSON mapping file.
* `Source Permissible Values Term`: If not specified (blank/empty), the harmonized value of this field will be set to
  the unmodified value retrieved from the input source data. If specified, the harmonized value will be set to the
  value of the matching `Target Permissible Values Term` property. Corresponds to the `replacement_values.old_value`
  property of the transformed output JSON mapping file.
* `Source Permissible Values Code`: Currently unused; reserved for potential use in the future.
* `Target Variable Name`: The name of the output field for which the harmonized data for this mapping will
  be saved. Corresponds to the `output_field` property of the transformed output JSON mapping file.
* `Target Permissible Values Term`: The harmonized value to use for the matching `Source Permissible Values Term`
  property. If the `Source Permissible Values Term` property is not specified then this property will be ignored
  and the harmonized value of this field will be set to the unmodified value retrieved from the input source data.
  Corresponds to the `replacement_values.new_value` property of the transformed output JSON mapping file.
* `Target Permissible Values Code`: Currently unused; reserved for potential use in the future.
* `Type Group Index`: An integer identifier to be used if this field's mapping will apply to multiple records. If
  more than one type group index is present in a mapping then multiple records of the target output node type will
  be created for each record present in the input source data. This is typically used for reference files, since
  multiple reference file entries (programmatic source code, transformation, schema) must be present for a single
  source. Corresponds to the `type_group_index` property of the transformed output JSON mapping file.
* `Default Value If Null/Blank`: The value to which the harmonized output field should be set when the input source
  data value of this field (`Source Variable Name`) is blank/null. Corresponds to the `default_value` property of
  the transformed output JSON mapping file.
* `Replacement Values`: A collection (python type hint: `dict[str, list[str]]`) of
  `[source value] => [list of replacement values]` pairs that can be used to apply on-demand corrections to invalid
  source data values during harmonization. For example for the CCDI UCSF `phs002430` study:
  `{"C80 : UNKNOWN PRIMARY SITE": ["C80.9 : Unknown primary site"]}`. Each `source => replacement value` pair will
  result in an entry being added to the `replacement_values` property of the transformed output JSON mappoing file.
  The effect can simulated by entering multiple mapping rows but using this column makes it more obvious for
  maintainers that it's intended for corrective replacement as opposed to explicit mapping of constrained/permissible
  source values as is the case for Target NBL and MCI (`diagnosis.diagnosis`, `diagnosis.anatomic_site`, etc).
