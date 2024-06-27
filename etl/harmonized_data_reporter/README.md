# C3DC harmonized data report creation script

This script analyzes the JSON harmonized data output files created by the C3DC ETL scripts and creates a tabular
(CSV) report file containing object counts similar to below:
| Study name | Participants | Diagnoses | Survival | Reference files |
| :---:      | :---:        | :---:     | :---:    | :---:           |
| phs002518  | 1039         | 1039      | 1039     | 4               |
 
Create/update the `.env` file in the same directory and then execute `python harmonized_data_reporter.py` to create
the harmonized data report file. The environment variables are described below; the sample .env_example can be used
as a starting point. A file with a name other than `.env` can be specified as a command line argument:
`python harmonized_data_reporter.py "/path/to/config/file"`

## Environment variables:
* `HARMONIZED_DATA_FILES`: A string-ified dictionary of key-value pairs that contain the name and harmonized data
file path for each study to be included in the report
* `REPORT_OUTPUT_PATH`: The path where the report file should be saved
