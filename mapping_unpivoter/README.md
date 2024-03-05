# Internal D4CG mapping unpivoter scripts

__CAUTION: be careful not to use this script to generate (overwrite) transformation/mapping JSON files that may be
directly authored/maintained__

The C3DC transformation/mapping rules are passed to the data harmonization scripts as a JSON file. The D4CG
team maintains the mappings in tabular format in an internal file repository and converts them to JSON prior
to executing the ETL scripts. This directory contains the python script that handles the conversion, which
can be performed by following the steps below:
1. Create/update the [JSON schema version](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/README.md) of the
[C3DC model](https://github.com/CBIIT/c3dc-model/tree/main/model-desc).
1. Download the mappings in the internal shared document as separate CSV files for each sheet in the document.
   The 'Microsoft Excel (.xlsx)' download option shortens and truncates the sheet/tab names, which makes it
   difficult for an automated script to detect the source-file => mapping entries. This may be supported in the
   future as more mappings are added and the time and effort required to manually download a CSV file for each
   mapped sheet increases.
1. Create/update the environment file in the same directory; environment variables are described below and the
   sample .env_example file can be used as a starting point and customized as needed
1. Execute the command `python mapping_unpivoter.py unpivot_transformation_mappings [.env file path]`

__NOTE__: `reference_file` records must be included in the final harmonized data output file for input files such
as the ETL script, transformation mapping file, and input source files. The reference file information that must
be present includes the relevant file's size and md5 hash. When the corresponding `reference_file` mappings are
present in the transformation mapping file, the ETL scripts can transform them into the necessary C3DC model
`reference_file` objects and inject them into the final harmonized data output file. The transformation mapping
file created by the mapping unpivoter script is one of the reference files for which file size and md5 hash must
be calculated and included in the harmonized data output file. These self-referential properties are calculated
for the transformation mapping file with these values initially set to 0 (file size) and blank string (md5 hash).
The respective properties (`reference_file.file_size`, `reference.md5sum`) must then be set to the calculated
values. Setting the properties back to 0 and '' respectively should then allow the previously calculated values to
be verified.

The mapping unpivoter script can be directed to set the `reference_file.file_size` and `reference_file.md5hash`
properties of the resulting transformation mapping file it creates by setting the
`AUTO_UPDATE_REFERENCE_FILE_MAPPINGS` configuration property to `True`. The script can also
be be directed to only calculate and set these values for the transformation mapping file by calling it with
the appropriate argument:
`python mapping_unpivoter.py update_reference_file_mappings [.env file path]`.
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
  * `mappings_file`: Path to the source tabular document (e.g. CSV) which will be converted to JSON
  * `source_data_file`: Optional path to the source data file for the transformation/mapping. If populated the
    mapping unpivoter script will include this file when updating the reference file size and md5 sum mappings
    properties.