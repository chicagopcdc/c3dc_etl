# Script to collate and pivot MCI study data in JSON files

## Pre-requisites
Install Python (3.11 used at time of documentation) and add support for dependencies such as
[PETL](https://github.com/petl-developers/petl) by, for example,
[creating and activating a Conda environment](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file)
based on the [Conda environment file](https://github.com/chicagopcdc/c3dc_etl/blob/main/conda_environment.yml)

## Execution Steps
Call the script with two arguments, the first the path to the file or a directory containing the JSON data to be
pivoted and the second the destination file where the pivoted data should be saved. If a directory is passed as
an input then data from all JSON files within that directory and sub-directories will be pivoted. If the destination
file already exists a copy will be saved in the same directory, e.g. `filename.xlsx` => `filename_last.xlsx`.

Example command:
```
python data_pivoter.py /path/to/cog_msi_ingested_json_files/ cog_mci_ingested_pivoted_data.xlsx
```
