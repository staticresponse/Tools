import os
from datetime import datetime
from typing import Any, Dict, List


class S3Inventory:
    """
    Prepare the metadata for ingestion based on the minimum requirements for an inventory record

    Requires the raw SQS message.
    
    """

    def __init__(self, record: Dict[str, Any]):
        s3_info = record.get("s3", {})
        bucket_info = s3_info.get("bucket", {})
        object_info = s3_info.get("object", {})

        self.bucket = bucket_info.get("name")
        self.key = object_info.get("key")
        self.eTag = object_info.get("eTag")
        self.size = object_info.get("size")
        self.event_time = self._parse_event_time(record)

        self.key_parts = self.key.split("/")
        self.team = self.key_parts[0] if len(self.key_parts) > 0 else None
        self.product = self.key_parts[1] if len(self.key_parts) > 1 else None

        self.filename = os.path.basename(self.key)
        self.file_ext = os.path.splitext(self.filename)[1][1:]

        self.storageinfo = {
            "bucket": self.bucket,
            "key": self.key,
            "s3uri": f"s3://{self.bucket}/{self.key}",
            "filename": self.filename,
            "file_size": self.size,
            "file_ext": self.file_ext,
            "object_date": self.event_time,
            "eTag": self.eTag,
            "file_depths": self._compute_file_depths()
        }

    def _parse_event_time(self, record: Dict[str, Any]) -> str:
        """
            Extract event time in iso format
        """
        time_val = record.get("eventTime")
        if time_val:
            try:
                return datetime.fromisoformat(time_val.replace("Z", "+00:00")).isoformat()
            except ValueError:
                pass
        return None

    def _compute_file_depths(self) -> List[Dict[str, Any]]:
        """
            generate the file depths into a list (for use in aggreagations later)
        """
        return [
            {"depth": i, "value": part}
            for i, part in enumerate(self.key_parts)
        ] if self.key_parts else []

    def to_dict(self) -> Dict[str, Any]:
        """
            Frame out the storage info record in a way that will be used to ingest
        """
        return {
            "team": self.team,
            "product": self.product,
            "storageinfo": self.storageinfo
        }

    def __repr__(self):
        return f"<S3Inventory team={self.team} product={self.product} key={self.key}>"
