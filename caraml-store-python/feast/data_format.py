from abc import ABC, abstractmethod

from feast.core.DataFormat_pb2 import FileFormat as FileFormatProto


class FileFormat(ABC):
    """
    Defines an abtract file forma used to encode feature data in files
    """

    @abstractmethod
    def to_proto(self):
        """
        Convert this FileFormat into its protobuf representation.
        """
        pass

    def __eq__(self, other):
        return self.to_proto() == other.to_proto()

    @classmethod
    def from_proto(cls, proto):
        """
        Construct this FileFormat from its protobuf representation.
        Raises NotImplementedError if FileFormat specified in given proto is not supported.
        """
        fmt = proto.WhichOneof("format")
        if fmt == "parquet_format":
            return ParquetFormat()
        raise NotImplementedError(f"FileFormat is unsupported: {fmt}")

    def __str__(self):
        """
        String representation of the file format passed to spark
        """
        raise NotImplementedError()


class ParquetFormat(FileFormat):
    """
    Defines the Parquet data format
    """

    def to_proto(self):
        return FileFormatProto(parquet_format=FileFormatProto.ParquetFormat())

    def __str__(self):
        return "parquet"
