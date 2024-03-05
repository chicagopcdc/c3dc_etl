# Childhood Cancer Clinical Data Commons (C3DC) data harmonization (ETL) scripts

The scripts and ancillary files in this repository are used to harmonize data for the
[C3DC data model](https://github.com/CBIIT/c3dc-model).

Incoming source data may be in a number of different formats:
* Target Neuroblastoma (Target NBL) source data is provided as two separate Excel (XLSX) files ('discovery' and
  'validation') where each row in a specific sheet represents a complete subject record.
* Molecular Characterization Initiative (MCI) source data is provided as a collection of individual JSON files,
  where each file represents a complete subject record.
* Childhood Cancer Data Initiative (CCDI) source data is provided in an Excel (XLSX) file containing individual
  sheets dedicated to each model node object, with each row representing a record of that type.

The specific steps may differ for each study 'family' but in general, each individual study will have a dedicated
transformation mapping collection stored in a JSON file and each study family will have a dedicated script that
implements the associated ETL and transformation mapping logic. The D4CG maintains field/property mappings in an
internally-maintained tabular document that is converted to the publicly-visible JSON format that is consumed by
the data harmonization script.

The general data harmonization process is as follows:
1. Convert [C3DC data model](https://github.com/CBIIT/c3dc-model) to JSON schema
1. Transform internal D4CG mapping rules to the JSON formatted transformation maping file (if the JSON
   transformation mapping file isn't being directly maintained)
1. Execute ETL script to harmonize source data in accordance with transformation mapping logic

The study-specific steps are described in more detail in the README files within each study family's subdirectory
underneath the `/etl/` directory.