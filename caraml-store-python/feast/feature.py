import re
from typing import List

from feast.serving.ServingService_pb2 import FeatureReference


def build_feature_references(feature_ref_strs: List[str]) -> List[FeatureReference]:
    """
    Builds a list of FeatureReference protos from a list of FeatureReference strings
    Args:
        feature_ref_strs: List of string feature references
    Returns:
        A list of FeatureReference protos parsed from args.
    """

    valid_feature_ref_regex = re.compile("[^:]+:[^:]+")
    for ref_str in feature_ref_strs:
        if not re.fullmatch(valid_feature_ref_regex, ref_str):
            raise ValueError("Feature reference should be in the form  of <feature table>:<feature name>")

    return [FeatureReference(feature_table=ref_str.split(':')[0], name=ref_str.split(":")[-1])
            for ref_str in feature_ref_strs]
