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
(JSON) data file by applying the mapping rules in the [JSON transformation/mapping file](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/target_nbl/transformations)
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
file will still be created but should be discarded) and the second step creating the 'final' harmonized data
output file.

### C3DC model JSON schema
The data harmonization script uses the [C3DC JSON schema file](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/schema.json)
for reference and validation, which is converted from the [C3DC model's](https://github.com/CBIIT/c3dc-model/tree/main/model-desc)
source YAML files. See the [C3DC JSON schema readme file](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/README.md)
for more information.

### Mapping unpivoter utility script
The mapping unpivoter script can be used to transform the development team's internal shared document containing
harmonized data mapping definitions to the [publicly available JSON transformation/mapping
deliverable](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/target_nbl/transformations), aka the 'remote'
configuration file described below referenced in the `transformations_url` property. The script can be used for
convenience in lieu of editing and maintaining the JSON transformation/mapping config file manually. See the
[readme file](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/README.md) for details.

## ETL script
The c3dc_etl.py script ingests the source data in tabular (XLSX) format and transforms it into a harmonized (JSON)
data file.

### Execution steps
Create a local file named `.env` as per the [example](https://github.com/chicagopcdc/c3dc_etl/blob/main/etl/mci/.env_example)
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

## Sample ETL execution shell script
The commands below can be adapted and executed in a shell script such as `c3dc_etl.sh` for convenience.
```
# Creation of MCI harmonized data file and transformation/mapping file involves the following steps:
# 1) If updating schema: update schema version var in ../../schema/.env and then either execute
#    ../../schema/schema_creator.py manually or uncomment schema script commands below
# 2) Update transformation/mapping version in ../../mapping_unpivoter/.env_mapping_unpivoter_mci_phs002790
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

# update version var in ../../mapping_unpivoter/.env_mapping_unpivoter_mci_phs002790 and then create base
# transformation/mapping file; assuming starting dir is './etl/mci' where '.' is project root dir
cd ../../mapping_unpivoter
python mapping_unpivoter.py unpivot_transformation_mappings .env_mapping_unpivoter_mci_phs002790
cp mapping_unpivoter.log mapping_unpivoter_mci_1.log

# run ETL script to create transformation/mapping file containing ref file entries for input source data files
cd ../etl/mci
python c3dc_etl.py

# save copy of current transformation/mapping file and overwrite with updated version created in last step
cp transformations/phs002790.v4.p1.json transformations/phs002790.v4.p1.json.bak
mv phs002790.v4.p1.ref_files.json transformations/phs002790.v4.p1.json

# update transformation/mapping file's reference file size and md5sum mappings
cd ../../mapping_unpivoter
python mapping_unpivoter.py update_reference_file_mappings .env_mapping_unpivoter_mci_phs002790
cp mapping_unpivoter.log mapping_unpivoter_mci_2.log

# run ETL script again to create final harmonized data file
cd ../etl/mci
python c3dc_etl.py
```
