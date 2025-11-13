import unittest
from datetime import datetime
from s3_inventory import S3Inventory

class TestS3Inventory(unittest.TestCase):

    def setUp(self):
        self.sample_event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test-bucket-t1"},
                        "object": {
                            "key": "1000000/GEODS00000/another/one/down/test.txt",
                            "size": 123456,
                            "eTag": "abcd1234efgh5678",
                        }
                    },
                    "eventTime": "2025-11-12T12:34:56.000Z"
                }
            ]
        }

    def test_inventory_parsing(self):
        """Test that the S3Inventory object correctly parses event data"""
        inv = S3Inventory.from_s3_event(self.sample_event)

        self.assertEqual(inv.team, "1000000")
        self.assertEqual(inv.product, "GEODS00000")
        self.assertEqual(inv.storageinfo["bucket"], "test-bucket-t1")
        self.assertEqual(inv.storageinfo["key"], "1000000/GEODS00000/another/one/down/test.txt")
        self.assertEqual(inv.storageinfo["s3uri"], "s3://test-bucket-t1/1000000/GEODS00000/another/one/down/test.txt")
        self.assertEqual(inv.storageinfo["file_size"], 123456)
        self.assertEqual(inv.storageinfo["file_ext"], ".txt")
        self.assertEqual(inv.storageinfo["eTag"], "abcd1234efgh5678")
        self.assertEqual(inv.storageinfo["filename"], "test.csv")
        self.assertEqual(inv.storageinfo["file_depths"][0], {"depth": 0, "value": "1000000"})
        self.assertEqual(inv.storageinfo["file_depths"][1], {"depth": 1, "value": "GEODS00000"})

        # Check date parsing
        self.assertIsInstance(inv.storageinfo["object_date"], datetime)
        self.assertEqual(inv.storageinfo["object_date"].isoformat(), "2025-11-12T12:34:56")

    def test_handles_nested_key(self):
        """Test that nested keys with deeper paths work correctly"""
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test-bucket-t1"},
                        "object": {
                            "key": "1000000/GEODS00000/another/one/down/test.txt",
                            "size": 999,
                            "eTag": "999etag",
                        }
                    },
                    "eventTime": "2025-11-11T10:00:00.000Z"
                }
            ]
        }

        inv = S3Inventory.from_s3_event(event)
        self.assertEqual(inv.team, "1000000")
        self.assertEqual(inv.product, "GEODS00000")
        self.assertEqual(inv.storageinfo["file_ext"], ".txt")
        self.assertEqual(inv.storageinfo["filename"], "test.txt")

    def test_missing_fields(self):
        """Test behavior when optional fields are missing"""
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "test-bucket-t1"},
                        "object": {
                            "key": "1000000/GEODS00000/test.txt",
                        }
                    },
                    "eventTime": "2025-11-12T00:00:00.000Z"
                }
            ]
        }

        inv = S3Inventory.from_s3_event(event)
        self.assertEqual(inv.storageinfo["file_size"], None)
        self.assertEqual(inv.storageinfo["eTag"], None)
        self.assertEqual(inv.storageinfo["file_ext"], ".txt")
        

    def test_invalid_key_format(self):
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "bucket"},
                        "object": {"key": "justafile.csv"}
                    },
                    "eventTime": "2025-11-12T00:00:00.000Z"
                }
            ]
        }
        inv = S3Inventory.from_s3_event(event)
        # Should default gracefully
        self.assertEqual(inv.team, "justafile.csv")
        self.assertIsNone(inv.product)


if __name__ == "__main__":
    unittest.main()
