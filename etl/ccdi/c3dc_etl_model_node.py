""" C3DC ETL Model Node Types """
from __future__ import annotations
from enum import Enum


class C3dcEtlModelNode(str, Enum):
    """
    Enum class for ETL timing sub-types
    """
    DIAGNOSIS = 'diagnosis'
    PARTICIPANT = 'participant'
    REFERENCE_FILE = 'reference_file'
    SAMPLE = 'sample'
    STUDY = 'study'
    SURVIVAL = 'survival'

    def __str__(self):
        return self.value

    @staticmethod
    def get(node_type: str) -> C3dcEtlModelNode:
        """ Get C3dcModelNode matching specified node_type or None if not found """
        try:
            return C3dcEtlModelNode[node_type.upper()]
        except KeyError:
            return None
