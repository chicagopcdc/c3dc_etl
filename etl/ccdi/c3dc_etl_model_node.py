""" C3DC ETL Model Node Types """
from __future__ import annotations
from enum import Enum


class C3dcEtlModelNode(str, Enum):
    """
    Enum class for ETL timing sub-types
    """
    DIAGNOSIS = 'diagnosis'
    GENETIC_ANALYSIS = 'genetic_analysis'
    LABORATORY_TEST = 'laboratory_test'
    PARTICIPANT = 'participant'
    REFERENCE_FILE = 'reference_file'
    SAMPLE = 'sample'
    STUDY = 'study'
    SURVIVAL = 'survival'
    TREATMENT = 'treatment'
    TREATMENT_RESPONSE = 'treatment_response'

    def __str__(self):
        return self.value

    @staticmethod
    def get(node_type: str) -> C3dcEtlModelNode:
        """ Get C3dcModelNode matching specified node_type or None if not found """
        try:
            return C3dcEtlModelNode[node_type.upper()]
        except KeyError:
            return None

    @staticmethod
    def get_pluralized_node_name(node_type: str) -> str:
        """ Get pluralized form of node name e.g. for output record set name """
        if node_type.endswith('is'):
            # diagnosis => diagnoses, genetic_analysis => genetic_analyses, etc
            return f'{node_type[:-2]}es'

        if node_type[-1] == 'y':
            # study => studies
            return f'{node_type[:-1]}ies'

        # participant => participants, etc
        return f'{node_type}s'
