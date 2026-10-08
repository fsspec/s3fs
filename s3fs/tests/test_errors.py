import pytest
from botocore.exceptions import ClientError

from s3fs.errors import translate_boto_error


@pytest.mark.parametrize("code", ["PreconditionFailed", "AccessDenied"])
@pytest.mark.parametrize("message", [None, ""])
def test_translate_error_without_message(code, message):
    error = ClientError({"Error": {"Code": code, "Message": message}}, "GetObject")

    translated = translate_boto_error(error)

    assert str(error) in str(translated)
    assert translated.__cause__ is error


def test_translate_error_preserves_explicit_message():
    error = ClientError(
        {"Error": {"Code": "PreconditionFailed", "Message": None}}, "GetObject"
    )

    translated = translate_boto_error(error, message="custom message", set_cause=False)

    assert translated.strerror == "custom message"
    assert translated.__cause__ is None
