import re
from typing import List, MutableMapping, Optional

from feast.core.Feature_pb2 import FeatureSpec as FeatureSpecProto
from feast.serving.ServingService_pb2 import FeatureReference as FeatureReferenceProto
from feast.types import Value_pb2 as ValueTypeProto
from feast.value_type import ValueType


class Feature:
    """Feature field type"""

    def __init__(
        self,
        name: str,
        dtype: ValueType,
        labels: Optional[MutableMapping[str, str]] = None,
    ):
        self._name = name
        if not isinstance(dtype, ValueType):
            raise ValueError("dtype is not a valid ValueType")
        self._dtype = dtype
        if labels is None:
            self._labels = dict()  # type: MutableMapping
        else:
            self._labels = labels

    def __eq__(self, other):
        if (
            self.name != other.name
            or self.dtype != other.dtype
            or self.labels != other.labels
        ):
            return False
        return True

    def __lt__(self, other):
        return self.name < other.name

    @property
    def name(self):
        """
        Getter for name of this field
        """
        return self._name

    @property
    def dtype(self) -> ValueType:
        """
        Getter for data type of this field
        """
        return self._dtype

    @property
    def labels(self) -> MutableMapping[str, str]:
        """
        Getter for labels of this field
        """
        return self._labels

    def to_proto(self) -> FeatureSpecProto:
        """Converts Feature object to its Protocol Buffer representation"""
        value_type = ValueTypeProto.ValueType.Enum.Value(self.dtype.name)

        return FeatureSpecProto(
            name=self.name,
            value_type=value_type,
            labels=self.labels,
        )

    @classmethod
    def from_proto(cls, feature_proto: FeatureSpecProto):
        """
        Args:
            feature_proto: FeatureSpecV2 protobuf object
        Returns:
            Feature object
        """

        feature = cls(
            name=feature_proto.name,
            dtype=ValueType(feature_proto.value_type),
            labels=feature_proto.labels,
        )

        return feature


def build_feature_references(
    feature_ref_strs: List[str],
) -> List[FeatureReferenceProto]:
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
            raise ValueError(
                "Feature reference should be in the form  of <feature table>:<feature name>"
            )

    return [
        FeatureReferenceProto(
            feature_table=ref_str.split(":")[0], name=ref_str.split(":")[-1]
        )
        for ref_str in feature_ref_strs
    ]
