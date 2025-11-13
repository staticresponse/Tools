import pytest
from sqs_parser import parse_sqs_message, unwrap_sns_message


@pytest.fixture
def direct_s3_event():
    return '''
    {
      "Records": [
        {
          "eventSource": "aws:s3",
          "s3": {
            "bucket": { "name": "test-bucket-t1" },
            "object": { "key": "test.txt" }
          }
        }
      ]
    }
    '''


@pytest.fixture
def sns_wrapped_event():
    return '''
    {
      "Type": "Notification",
      "Message": "{\\"Records\\":[{\\"eventSource\\":\\"aws:s3\\",\\"s3\\":{\\"bucket\\":{\\"name\\":\\"test-bucket-t1\\"},\\"object\\":{\\"key\\":\\"wrapped.csv\\"}}}]}"
    }
    '''


@pytest.fixture
def malformed_json():
    return '{"Records": [ {"eventSource": "aws:s3", "s3": { "bucket": {"name": "bad-bucket"} } ]'


@pytest.fixture
def unsupported_message():
    return '{"SomeOtherField": "value"}'


def test_unwrap_sns_valid(sns_wrapped_event):
    records = unwrap_sns_message(sns_wrapped_event)
    assert len(records) == 1
    assert records[0]["s3"]["bucket"]["name"] == "test-bucket-t1"
    assert records[0]["s3"]["object"]["key"] == "wrapped.csv"


def test_unwrap_sns_invalid_format(unsupported_message):
    with pytest.raises(ValueError):
        unwrap_sns_message(unsupported_message)


def test_parse_direct_s3(direct_s3_event):
    records = parse_sqs_message(direct_s3_event)
    assert len(records) == 1
    assert records[0]["s3"]["bucket"]["name"] == "test-bucket-t1"
    assert records[0]["s3"]["object"]["key"] == "test.txt"


def test_parse_sns_wrapped(sns_wrapped_event):
    records = parse_sqs_message(sns_wrapped_event)
    assert len(records) == 1
    assert records[0]["s3"]["bucket"]["name"] == "test-bucket-t1"


def test_parse_malformed_json(malformed_json):
    with pytest.raises(ValueError):
        parse_sqs_message(malformed_json)


def test_parse_unsupported_structure(unsupported_message):
    with pytest.raises(ValueError):
        parse_sqs_message(unsupported_message)
