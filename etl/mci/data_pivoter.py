""" JSON Data Pivoter """
import json
import logging
import logging.config
import os
import pathlib
import sys
import warnings

import petl


# suppress openpyxl warning about inability to parse header/footer
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

_logger = logging.getLogger(__name__)
if _logger.hasHandlers():
    _logger.handlers.clear()

logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s]: %(message)s"
        }
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
        "file": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.FileHandler",
            "filename": "data_pivoter.log",
            "mode": "w"
        }
    },
    "loggers": {
        "": { # root logger
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False
        },
        "__main__": {
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False
        }
    }
})


def pivot_json_data_to_xlsx(input_path: str, output_path: str) -> None:
    """ Convert (pivot) JSON file(s) at specified source path into XLSX file at output path """
    # collate source files if directory specified instead of individual file
    source_paths: list[str] = []
    if os.path.isfile(input_path):
        source_paths.append(input_path)
        if not input_path.endswith('.json'):
            _logger.warning('File extension not JSON as expected')
    else:
        _logger.info('Finding all JSON files within %s', input_path)
        dir_path: str
        file_names: list[str]
        for dir_path, _, file_names in os.walk(input_path):
            source_paths.extend(os.path.join(dir_path, f) for f in file_names if f.endswith('.json'))

    _logger.info('Converting %d file(s) to XLSX from %s', len(source_paths), input_path)

    # collection of dictionaries per form for conversion to PETL tables per form
    all_form_dict_lists: dict[str, list[dict[str, any]]] = {}
    form_id: str
    all_form_dict_list: list[dict[str, any]]

    files_processed: int = 0

    source_path: str
    for source_path in sorted(source_paths):
        files_processed += 1
        if files_processed % 100 == 0:
            _logger.info('%d files processed', files_processed)

        # load subject json source data
        subject_data: dict[str, any]
        with open(source_path, mode='r', encoding='utf-8') as json_file:
            subject_data = json.load(json_file)
        if not subject_data:
            _logger.warning('Unable to load JSON data from "%s", skipping', source_path)
            continue

        # verify upi (usi) available for use as subject id
        upi: str = subject_data.get('upi')
        if not upi:
            _logger.warning('No root-level "upi" value found in file "%s", skipping', source_path)
            continue

        # collate data for all available forms
        form: dict[str, any]
        for form in subject_data.get('forms', []):
            form_id = form.get('form_id')
            if not form_id:
                _logger.warning('Form without "form_id" defined in file "%s", skipping', source_path)
                continue

            subject_form_dict: dict[str, any] = {'upi': upi}
            form_data: dict[str, any]
            for form_data in form.get('data', []):
                subject_form_dict.update({form_data.get('form_field_id'):form_data.get('value')})

            # append the data for this subject and form to the overall data set for this form
            all_form_dict_list = all_form_dict_lists.get(form_id, [])
            all_form_dict_list.append(subject_form_dict)
            all_form_dict_lists[form_id] = all_form_dict_list

    if not all_form_dict_lists:
        _logger.warning('No form data loaded')
        return

    # save copy of output file if already present
    if os.path.exists(output_path):
        out_path: pathlib.Path = pathlib.Path(output_path)
        out_path.rename(out_path.with_name(f'{out_path.stem}_last{out_path.suffix}'))

    _logger.info('Saving data to %s', output_path)

    # build composite table from all form fields for each subject and save to first sheet
    _logger.info('Saving full-width joined records to first sheet')
    tbl_all: any = None
    for all_form_dict_list in all_form_dict_lists.values():
        tbl: any = petl.fromdicts(all_form_dict_list)
        tbl_all = tbl if not tbl_all else petl.leftjoin(tbl_all, tbl, 'upi')
    new_header: list[str] = sorted(list(petl.header(tbl_all)))
    new_header.insert(0, new_header.pop(new_header.index('upi')))
    tbl_all = petl.cut(tbl_all, new_header)
    petl.toxlsx(tbl_all, output_path, '_ALL_FORMS_', 'add')

    # save individual form data as new sheets
    xlsx_sheet_names: set[str] = set()
    for form_id, all_form_dict_list in sorted(all_form_dict_lists.items()):
        _logger.info('Saving data for form "%s"', form_id)
        tbl: any = petl.fromdicts(all_form_dict_list)
        new_header = sorted(list(petl.header(tbl)))
        new_header.insert(0, new_header.pop(new_header.index('upi')))
        tbl = petl.cut(tbl, new_header)
        xlsx_sheet_name: str = form_id if len(form_id) < 31 else form_id[:30]
        if len(form_id) > 30:
            _logger.warning(
                'Form id "%s" too long to use as Excel sheet name, truncating to "%s"',
                form_id,
                xlsx_sheet_name
            )
        if xlsx_sheet_name in xlsx_sheet_names:
            _logger.warning('Duplicate Excel sheet name. skipping: %s (%s)', xlsx_sheet_name, form_id)
            continue
        xlsx_sheet_names.add(xlsx_sheet_name)

        petl.toxlsx(tbl, output_path, xlsx_sheet_name, 'add')

    _logger.info('JSON to XLS pivot complete')


def print_usage() -> None:
    """ Print script usage """
    _logger.info('usage: python %s [source file or directory] [output file]', sys.argv[0])


def main() -> None:
    """ Script entry point """
    if len(sys.argv) != 3:
        print_usage()
        return

    source_path: str = sys.argv[1]
    output_path: str = sys.argv[2]
    pivot_json_data_to_xlsx(source_path, output_path)


if __name__ == '__main__':
    main()
