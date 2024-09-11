""" Row mapped node builder for C3DC MCI ETL """
import ast
import csv
import logging
import logging.config
from typing import Callable, Self
import uuid
import warnings

from c3dc_etl_model_node import C3dcEtlModelNode


# suppress openpyxl warning about inability to parse header/footer
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')


class C3dcRowMappedBuilder:
    """
    Build C3DC MCI ETL node records such as treatment and treatment_response
    that are mapped in rows, with one harmonized record per matching row
    """
    SUBJECT_ID_FIELD: str = 'upi'
    DEFAULT_AGE: int = -999

    NODE_SOURCE_VARIABLE_FIELDS: dict[C3dcEtlModelNode, dict[str, str]] = {
        C3dcEtlModelNode.TREATMENT: {
            'Source Variable Name': 'Source Variable Value'
        },
        C3dcEtlModelNode.TREATMENT_RESPONSE: {
            'Source Variable Name 1': 'Source Variable Value 1',
            'Source Variable Name 2': 'Source Variable Value 2'
        },
    }

    def __init__(
        self,
        node: C3dcEtlModelNode,
        source_variable_fields: dict[str, str],
        generate_uuid_callback: Callable[[], uuid.UUID] = None,
        convert_output_value_callback: Callable[[str, any], any] = None,
        is_output_property_required_callback: Callable[[str], bool] = None,
        mappings: list[dict[str, str | int]] = None,
        logger: logging.Logger = None
    ) -> None:
        self.node: C3dcEtlModelNode = node
        self.source_variable_fields: dict[str, str] = source_variable_fields or {}
        self.generate_uuid_callback: Callable[[], uuid.UUID] = generate_uuid_callback
        self.convert_output_value_callback: Callable[[str, any], any] = convert_output_value_callback
        self.is_output_property_required_callback: Callable[[str], bool] = is_output_property_required_callback
        self.mappings = mappings or []
        self.logger: logging.Logger = logger or logging.getLogger(__name__)
        self._node_field_required: dict[str, bool] = {}

    @property
    def mappings(self) -> list[dict[str, str | int]]:
        """ get mapping rules to apply to build node records """
        return self._mappings

    @mappings.setter
    def mappings(self, value: list[dict[str, str | int]]) -> None:
        """ set mapping rules to apply to build node records """
        self._mappings: list[dict[str, str | int]] = value
        if not self._mappings:
            return

        self._validate_mappings()
        self._node_field_required.clear()
        node_field: str
        for node_field in [
            f for f in set().union(*(m.keys() for m in self.mappings))
                if f.startswith(f'{self.node}.')
        ]:
            self._node_field_required[node_field] = self._is_output_property_required(node_field)

    @staticmethod
    def get_instance(node: C3dcEtlModelNode) -> Self:
        """ Create builder for specified node type """
        if node not in C3dcRowMappedBuilder.NODE_SOURCE_VARIABLE_FIELDS:
            raise NotImplementedError(
                f'{C3dcRowMappedBuilder.__name__}.{C3dcRowMappedBuilder.get_instance.__name__}: ' +
                f'node "{node}" is not supported'
            )

        return C3dcRowMappedBuilder(node, source_variable_fields=C3dcRowMappedBuilder.NODE_SOURCE_VARIABLE_FIELDS[node])

    @staticmethod
    def is_number(value: str) -> bool:
        """ Determine whether specified string is number (float or int) """
        try:
            float(value)
        except (TypeError, ValueError):
            return False
        return True

    def sum_abs_first(self, *args: int | float | str | None) -> int | None:
        """ Return sum of addends, using absolute value of first """
        sum_method_name: str = f'{C3dcRowMappedBuilder.sum_abs_first.__name__}'
        if not args:
            raise RuntimeError(f'{self.node} builder: no addends specified for macro "{sum_method_name}"')

        # strip extra spaces; csv module parses "field 1, field 2" into ["field 1", " field 2"]
        addends: list[float | int] = []
        idx: int
        arg: any
        for idx, arg in enumerate(args):
            addend: str = '' if arg is None else str(arg).strip()
            if addend in (None, ''):
                #self._logger.warning('%s: addend %d null/empty', sum_method_name, idx)
                return None
            if not C3dcRowMappedBuilder.is_number(addend):
                self.logger.warning(
                    '%s builder: %s addend %d ("%s") not a number',
                    self.node,
                    sum_method_name,
                    idx,
                    arg
                )
                return None
            addend = float(addend)
            addend = abs(addend) if not addends else addend
            addends.append(addend if not addend.is_integer() else int(addend))
        return sum(addends)

    def _generate_uuid(self) -> uuid.UUID:
        """ generate UUID by calling callback """
        if self.generate_uuid_callback is None:
            raise RuntimeError('{self.node} builder: uuid generation callback not specified')
        return self.generate_uuid_callback()

    def _convert_output_value(self, node_type_dot_property_name: str, value: any) -> any:
        """ convert specified value to type valid for property (in type.property notation) by calling callback """
        if self.convert_output_value_callback is None:
            raise RuntimeError(f'{self.node} builder: value conversion callback not specified')
        return None if value is None else self.convert_output_value_callback(node_type_dot_property_name, value)

    def _is_output_property_required(self, node_type_dot_property_name: str) -> bool:
        """ check if specified property (in type.property notation) is required by calling callback """
        if self.is_output_property_required_callback is None:
            raise RuntimeError(
                f'{self.node} builder: is_output_property_required callback not available, ' +
                'specify before setting mappings'
            )
        return self.is_output_property_required_callback(node_type_dot_property_name)

    def _validate_mappings(self, raise_on_error: bool = True) -> None:
        """ Check for errors in mapping rules """
        mapping_errors: list[str] = self._get_mapping_errors()
        if not mapping_errors:
            return
        self.logger.fatal(f'{self.node} mapping errors found:')
        mapping_error: str
        for mapping_error in mapping_errors:
            self.logger.error(mapping_error)
        if raise_on_error:
            raise RuntimeError(f'{self.node} builder: {self.node} mapping errors found: {mapping_errors}')

    def _get_mapping_errors(self) -> list[str]:
        """ Get collection of errors in mapping rules """
        errors: list[str] = []
        if not self.mappings:
            errors.append(f'No {self.node} mappings specified')

        required_fields: set[str] = {
            *self.source_variable_fields.keys(),
            *self.source_variable_fields.values()
        }

        mappings_seen: list[dict[str, any]] = []

        index: int
        mapping: dict[str, str | int]
        for index, mapping in enumerate(self.mappings):
            mapping_num = index + 1
            # validate mapping not null/empty
            if not mapping:
                errors.append(f'{self.node} mapping #{mapping_num} not specified')
                continue

            # verify required fields present
            if not required_fields.issubset(mapping.keys()) or not any(k.startswith(f'{self.node}.') for k in mapping):
                errors.append(
                    f'Invalid {self.node} mapping #{mapping_num}, must define required fields {required_fields} ' +
                    f'and one or more fields beginning with "{self.node}."'
                )
                continue

            # verify no duplicates present
            if any(m == mapping for m in mappings_seen):
                errors.append(f'{self.node} mapping #{mapping_num} is a duplicate')
                continue

            mappings_seen.append(mapping)
        return errors

    def _is_mapping_match(self, mapping: dict[str, str | int], source_record: dict[str, any]) -> bool:
        """ Determine whether specified source record matches mapping criteria """
        # check if mapped field name(s) present in source record
        if not {mapping.get(f) for f in self.source_variable_fields}.issubset(source_record.keys()):
            return False

        # check if mapped value(s) and source value(s) are both collections
        source_variable_name: str
        source_variable_value: str
        for source_variable_name, source_variable_value in self.source_variable_fields.items():
            mapping_field: str = mapping.get(source_variable_name)
            mapping_value: any = mapping.get(source_variable_value)
            mapping_value_is_collection: bool = isinstance(mapping_value, (list, tuple))
            source_record_value: any = source_record.get(mapping_field, '')
            source_record_value_is_collection: bool = isinstance(source_record_value, (list, tuple))
            # pylint: disable=too-many-boolean-expressions
            if (
                (mapping_value_is_collection and not source_record_value_is_collection)
                or
                (not mapping_value_is_collection and source_record_value_is_collection)
                or
                (
                    mapping_value_is_collection
                    and
                    source_record_value_is_collection
                    and
                    len(mapping_value) != len(source_record_value)
                )
            ):
                return False

            # compare mapped value and source value by ordinal index if collections
            if mapping_value_is_collection and source_record_value_is_collection:
                is_match: bool = True
                index: int
                src_rec_val: str
                for index, src_rec_val in enumerate(source_record_value):
                    mapped_val: str = mapping_value[index]
                    mapped_val = '' if mapped_val is None else str(mapped_val).strip().casefold()
                    src_rec_val = '' if src_rec_val is None else str(src_rec_val).strip().casefold()

                    if src_rec_val != mapped_val:
                        is_match = False
                        break
                if not is_match:
                    return False

            # compare mapped value and source value
            mapping_value = '' if mapping_value is None else str(mapping_value).strip().casefold()
            source_record_value = '' if source_record_value is None else str(source_record_value).strip().casefold()
            if not (
                (mapping_value in (None, '') and source_record_value in (None, ''))
                or
                mapping_value == source_record_value
            ):
                return False

        return True

    @staticmethod
    def _parse_macro(macro: str) -> tuple[str, list[any]]:
        """ Parse macro and return tuple containing function name and list of source variable name arguments """
        macro = macro.strip(' {}').strip()
        ast_node: ast.Module = ast.parse(macro)
        func_call: ast.Call = ast_node.body[0].value
        func_name: str = func_call.func.id if isinstance(func_call, ast.Call) else macro
        args: list[any] = []
        if hasattr(func_call, 'args'):
            arg: any
            for arg in func_call.args:
                args.append(arg.id)
        return (func_name, args)

    def get_mapped_source_fields(self) -> list[str]:
        """ Get collection of all source fields in mappings """
        self._validate_mappings()

        source_fields: set[str] = set()
        mapping: dict[str, any]
        for mapping in self.mappings:
            source_variable_name_field: str
            for source_variable_name_field in self.source_variable_fields:
                source_variable_name: str = mapping.get(source_variable_name_field)
                if source_variable_name.startswith('[') and source_variable_name.endswith(']'):
                    # composite/derived source field, strip extra spaces; note csv
                    # module parses "field1, field2" into ["field1", " field2"]
                    sub_fields: list[str] = [s.strip() for s in next(csv.reader([source_variable_name.strip(' []')]))]
                    sub_field: str
                    for sub_field in sub_fields:
                        source_fields.add(sub_field)
                else:
                    source_fields.add(source_variable_name)

            # check for source variables specified in other fields e.g. as macro
            mapping_value: any
            for mapping_value in mapping.values():
                if not (str(mapping_value).startswith('{') and str(mapping_value).endswith('}')):
                    continue
                macro: str = mapping_value.strip(' {}').strip()
                func_args: list[any]
                (_, func_args) = self._parse_macro(macro)
                func_arg: any
                for func_arg in func_args:
                    if not (
                        C3dcRowMappedBuilder.is_number(func_arg)
                        or
                        (
                            str(func_arg).startswith(('"', '\''))
                            and
                            str(func_arg).endswith(('"', '\''))
                        )
                    ):
                        # macro arg is source variable name
                        source_fields.add(func_arg)

        return sorted(list(source_fields))

    def get_records(self, source_record: dict[str, any]) -> list[dict[str, any]]:
        """ Get row mapped node records for specified source record by applying mapping rules """
        self._validate_mappings()

        if not source_record:
            raise RuntimeError(f'{self.node} builder: no source record specified')

        records: list[dict[str, any]] = []
        mapping: dict[str, str | int]
        for mapping in self.mappings:
            if not self._is_mapping_match(mapping, source_record):
                continue

            # mapping matches source record, create node record and append to return list
            record: dict[str, any] = {}
            mapping_field: str
            mapping_value: any
            for mapping_field, mapping_value in mapping.items():
                if not mapping_field.startswith(f'{self.node}.'):
                    continue

                output_value: any = None

                if not (str(mapping_value).startswith('{') and str(mapping_value).endswith('}')):
                    # not mapped to macro e.g. sum_abs_first, set and proceed to next mapping field
                    output_value = self._convert_output_value(mapping_field, mapping_value)
                else:
                    # mapped to macro
                    macro: str = mapping_value.strip(' {}').strip()
                    func_name: str
                    func_args: list[any]
                    (func_name, func_args) = self._parse_macro(macro)
                    match func_name.lower():
                        case 'sum_abs_first':
                            arg_values: list[any] = []
                            func_arg: any
                            for func_arg in func_args:
                                arg_value: any = func_arg
                                # use macro func arg as direct (scalar) value if number or enclosed in quotes else
                                # treat as source variable name and retrieve from source record
                                if not (
                                    C3dcRowMappedBuilder.is_number(func_arg)
                                    or
                                    (
                                        str(func_arg).startswith(('"', '\''))
                                        and
                                        str(func_arg).endswith(('"', '\''))
                                    )
                                ):
                                    arg_value = source_record.get(func_arg)
                                arg_values.append(arg_value)
                            if not arg_values:
                                raise RuntimeError(f'{self.node} builder: macro "{func_name}" requires at least 1 arg')
                            sum_result: int | None = self.sum_abs_first(*arg_values)
                            output_value = sum_result if sum_result is not None else C3dcRowMappedBuilder.DEFAULT_AGE
                        case 'uuid':
                            output_value = str(self._generate_uuid())
                        case _:
                            output_value = mapping_value
                output_value = self._convert_output_value(mapping_field, output_value)
                if output_value in ('', None, []) and self._node_field_required.get(mapping_field, False):
                    self.logger.error(
                        (
                            '%s builder: required field "%s" empty/null, unable to build %s for source record "%s": '
                        ),
                        self.node,
                        mapping_field,
                        self.node,
                        source_record.get(
                            'source_file_name', f'{source_record[C3dcRowMappedBuilder.SUBJECT_ID_FIELD]}.json'
                        )
                    )
                record[mapping_field[len(f'{self.node}.'):]] = output_value
            records.append(record)

        return records


def main() -> None:
    """ Standalong entry point not supported """
    raise RuntimeError('Standalone script execution not supported')


if __name__ == '__main__':
    main()
