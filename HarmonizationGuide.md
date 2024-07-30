# Childhood Cancer Clinical Data Commons (C3DC) data harmonization guide

## Overview
The C3DC data harmonization process is implemented as a collection of Python scripts that apply validation and
mapping rules to transform source input data into harmonized output data containing JSON records that are conformant
with the [C3DC data model](https://github.com/CBIIT/c3dc-model).

The primary data transformation script, aka the ETL script, has three input dependencies:
* Source data to be harmonized for each study. There are currently three different study groups, each with different
  source data formats, that are supported for harmonization:
  * Childhood Cancer Data Initiative (CCDI) source data is contained in a multi-tabbed XLSX file based on the
  [CCDI submission template](https://github.com/CBIIT/ccdi-model/tree/main/metadata-manifest)
  * Molecular Characterization Initiative (MCI e.g. `phs002790`) source data is provided as a collection of
    JSON files, with one JSON source file for each subject.
  * Therapeutically Applicable Research to Generate Effective Treatments (TARGET) source data is contained in two
    XLSX files per study, one for 'discovery' and the other for 'validation'.
* A JSON schema file representation of the [C3DC data model](https://github.com/CBIIT/c3dc-model) used by the ETL
  script to verify the harmonized output data. The JSON schema file used to harmonize data delivered by the D4CG team
  is [published in GitHub](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/schema.json).
* A JSON file containing the data transformation/mapping rules to be applied to the input source data. The mapping
  rules are stored in an Excel (XLSX) file for readability and ease of maintenance and are converted into
  study-specific JSON files for use by the ETL script. The XLSX-formatted mapping rules used to harmonize data
  delivered by the D4CG team are [published in GitHub](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/C3DC%20Mappings.xlsx)
  as well as the study-specific JSON conversions for the
  [CCDI](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/ccdi/transformations),
  [MCI](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/mci/transformations), and
  [TARGET](https://github.com/chicagopcdc/c3dc_etl/tree/main/etl/target/transformations) study groups.

This guide lists the steps needed to create the JSON schema and transformation/mapping files and pass them to the ETL
script to create the final harmonized data output file for a given source input data set.

## Prepare harmonization environment
The Python script interpreter and dependencies can be installed using the Miniconda aka conda package manager.
1. Download and install the [Git](https://git-scm.com/downloads) source code control system if not already installed.
1. Download the C3DC data harmonization scripts and dependencies from the
   [GitHub repository](https://github.com/chicagopcdc/c3dc_etl). For example from a command prompt or git bash:
   `git clone https://github.com/chicagopcdc/c3dc_etl.git`.
   Note the local download location; this will be referred to throughout this guide as the "`c3dc_etl` root directory".
1. Download and install the [Miniconda (aka conda)](https://docs.anaconda.com/miniconda/miniconda-install/)
   Python package manager.
1. Open a conda terminal or command prompt per the installation instructions. The command line prompt should
   contain `(base)` for example `(base) user@machine` or `(base) C:\>`.
1. Create the runtime environment needed for harmonization using the
   [conda_enviroment.yml](https://github.com/chicagopcdc/c3dc_etl/blob/main/conda_environment.yml) file. Execute the
   following command from the `c3dc_etl` root directory where the C3DC data harmonization scripts were downloaded
   from GitHub: `conda env create -f conda_environment.yml`
1. Activate the newly created conda environment: `conda activate c3dc_etl`

## Create JSON schema file
The JSON schema file representation of the [C3DC data model](https://github.com/CBIIT/c3dc-model) is created by the
`schema_creator.py` script located in the `schema` subdirectory of the `c3dc_etl` root directory.
1. Create a `.env` config file in the `schema` subdirectory of the `c3dc_etl` root directory per the directions in the
   [README file](https://github.com/chicagopcdc/c3dc_etl/blob/main/schema/README.md). An example file named
   `.env_example` should already be present that can be copied and adjusted as needed. An example of an actual
   `.env` config file is included below.
   ```
   PROPS_SOURCE_URL='https://raw.githubusercontent.com/CBIIT/c3dc-model/dev/model-desc/c3dc-model-props.yml'
   NODES_SOURCE_URL='https://raw.githubusercontent.com/CBIIT/c3dc-model/dev/model-desc/c3dc-model.yml'
   META_SCHEMA_URL='https://json-schema.org/draft/2020-12/schema'
   SCHEMA_FILE_PATH='./schema.json'
   SCHEMA_ROOT_URL='https://datascience.cancer.gov/schemas/c3dc/'
   SCHEMA_ROOT_ID='2024-07-24'
   SCHEMA_ROOT_DESCRIPTION='C3DC schema bundle'
   SCHEMA_ROOT_COMMENT='Based on yaml schema files at https://github.com/CBIIT/c3dc-model/tree/main/model-desc'
   SCHEMA_ROOT_NODE='nodes'
   ```
1. Open a conda terminal or command prompt with the `c3dc_etl` conda environment activated as described in the earlier
   environment preparation step.
1. Change to the `schema` subdirectory of the `c3dc_etl` root directory.
1. Create the JSON schema file by executing the following command: `python schema_creator.py .env` \
   Any errors will be displayed in both the console output as well as the `schema_creator.log` log file in the same
   directory.
1. Verify that the JSON schema file, e.g. `schema.json` for the `.env` example file above, has been created in the
   location specified by the `SCHEMA_FILE_PATH` configuration variable and note the location so that it can be passed
   to subsequent scripts as a configuration parameter.

## Create JSON transformation/mapping file
The data transformation/mapping rules for all of the studies to be harmonized are stored in an Excel (XLSX) file for
readability and ease of maintenance. The rules encoded in the XLSX file are then converted to JSON format for use by
the ETL script. This conversion is performed by the `mapping_unpivoter.py` script loaded in the `mapping_unpivoter`
subdirectory of the `c3dc_etl` root directory.
1. Create a `.env` config file in the `mapping_unpivoter` subdirectory of the `c3dc_etl` root directory per the
   directions in the [README file](https://github.com/chicagopcdc/c3dc_etl/blob/main/mapping_unpivoter/README.md).
   An example file named `.env_example` should already be present that can be copied and adjusted as needed. Note that
   a local file path such as the location of the JSON schema file created in the previous step can be specified for
   the `JSON_SCHEMA_URL` variable. An example of an actual `.env` config file is included below. Note the use of
   slashes (`/`) instead of backslashes (`\`) for Windows paths.
   ```
   VERSION='20240724.1'
   JSON_SCHEMA_URL='C:/c3dc/c3dc_etl/schema/schema.json'
   OUTPUT_FILE='C:/c3dc/c3dc_etl/etl/ccdi/transformations/phs000720.json'
   ETL_SCRIPT_FILE='C:/c3dc/c3dc_etl/etl/ccdi/c3dc_etl.py'
   AUTO_UPDATE_REFERENCE_FILE_MAPPINGS='True'
   TRANSFORMATION_MAPPINGS_FILES='[
       {
           "transformation_name": "phs000720",
           "mappings_file": "C:/c3dc/c3dc_etl/mapping_unpivoter/C3DC Mappings.xlsx",
           "mappings_file_sheet": "phs000720",
           "source_data_file": "C:/c3dc/source_data/CCDI/phs000720/CCDI_Submission_Template_v1.7.2_phs000720_JoinRy_2024-05-30.xlsx"
       }
   ]'
   ```
1. Open a conda terminal or command prompt with the `c3dc_etl` conda environment activated as described in the earlier
   environment preparation step.
1. Change to the `mapping_unpivoter` subdirectory of the `c3dc_etl` root directory.
1. Create the transformation/mapping file by executing the following command:
   `python mapping_unpivoter.py unpivot_transformation_mappings .env` \
   Any errors will be displayed in both the console output as well as the `mapping_unpivoter.log` log file in the
   same directory.
1. Verify that the transformation/mapping file, e.g. `phs000720.json` for the `.env` example file above, has been
   created in the location specified by the `OUTPUT_FILE` configuration variable and note the location so that it
   can be passed to the ETL script as a configuration parameter.

## Create the harmonized data output file
The harmonized data output file is created by the `c3dc_etl.py` ETL script. Each study group has a distinct
subdirectory and ETL script within the `etl` subdirectory of the `c3dc_etl` root directory: `etl/ccdi`
for CCDI, `etl/mci` for MCI, and `etl/target` for TARGET. The ETL script that matches the input source data's study
group must be executed in order to create a valid harmonized data output file.
1. Create a `.env` config file per the directions in the README file for the particular study group
   ([CCDI](https://github.com/chicagopcdc/c3dc_etl/blob/main/etl/ccdi/README.md),
   [MCI](https://github.com/chicagopcdc/c3dc_etl/blob/main/etl/mci/README.md),
   [TARGET](https://github.com/chicagopcdc/c3dc_etl/blob/main/etl/target/README.md))
   An example file named `.env_example` should already be present that can be copied and adjusted as needed. Note that
   local file paths such as the locations of the JSON schema and transformation/mapping file created in the previous
   steps can be specified for the `JSON_SCHEMA_URL` and `transformations_url` variables respectively. An example of an
   actual `.env` config file (for the `phs000720` CCDI study) is included below.
   ```
   JSON_SCHEMA_URL='C:/Users/schoi/Documents/Work/Projects/scratch/c3dc/c3dc_etl/schema/schema.json'
   STUDY_CONFIGURATIONS='[
       {
           "study": "phs000720",
           "active": true,
           "transformations_url": "C:/c3dc/c3dc_etl/etl/ccdi/transformations/phs000720.json",
           "transformations": [
               {
                   "name": "phs000720",
                   "source_file_path": "C:/c3dc/data/source/CCDI/phs000720/CCDI_Submission_Template_v1.7.2_phs000720_JoinRy_2024-05-30.xlsx",
                   "output_file_path": "C:/c3dc/data/harmonized/phs000720.harmonized.json",
                   "uuid_seed": "phs000720",
                   "active": true
               }
           ]
       }
   ]'
   ```
1. Open a conda terminal or command prompt with the `c3dc_etl` conda environment activated as described in the earlier
   environment preparation step.
1. Change to the `etl/[study group]` subdirectory of the `c3dc_etl` root directory, for example `etl/ccdi`.
1. Create the harmonized data output file by executing the following command: `python c3dc_etl.py .env` \
   Any errors will be displayed in both the console output as well as the `c3dc_etl.log` log file in the same directory.
1. Verify that the harmonized data file, e.g. `phs000720.harmonized.json` for the `.env` example file above, has been
   created in the location specified by the `output_file_path` configuration variable.
