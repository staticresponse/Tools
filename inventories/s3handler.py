import json
from typing import Any, Dict, List


def unwrap_sns(message_body: str) -> List[Dict[str, Any]]:
    """
    Utility to extract the raw SQS message from an SNS message
    
    """
    try:
        msg = json.loads(message_body)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON in SNS message body")

    if msg.get("Type") == "Notification" and "Message" in msg:
        try:
            inner = json.loads(msg["Message"])
            if "Records" in inner:
                return inner["Records"]
        except json.JSONDecodeError:
            raise ValueError("SNS 'Message' field is not valid JSON")

    raise ValueError("Message is not a valid SNS notification containing S3 events")


def parse_sqs(message_body: str) -> List[Dict[str, Any]]:
    """
    Parse the SQS recueved message.

    Logic:
      1. Try to parse as direct S3 event
      2. On failure, attempt SNS unwrap
      3. If still failure, throw error

    Args:
        message_body: The raw SQS message body string
    Returns:
        A list of S3 event records
    Raises:
        ValueError: If both attempts fail
    """
    try:
        msg = json.loads(message_body)
        if "Records" in msg:
            return msg["Records"]
        else:
            # Message is either invalid or wrapped SNS
            raise ValueError("No 'Records' key found")
    except (json.JSONDecodeError, ValueError):
        try:
            return unwrap_sns(message_body)
        except ValueError:
            raise ValueError("Unrecognized SQS message format (not S3 or SNS-wrapped S3)")
