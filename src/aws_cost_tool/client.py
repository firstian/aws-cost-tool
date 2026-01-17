import io
import logging
import subprocess
import sys
from contextlib import redirect_stderr

import boto3
from botocore.exceptions import (
    ClientError,
    NoCredentialsError,
    SSOTokenLoadError,
    TokenRetrievalError,
    UnauthorizedSSOTokenError,
)

# Make sure that when the credential expires, boto3 doesn't dump a bunch of logs
# in the console that don't help.
logging.getLogger("boto3").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)


def check_aws_auth(profile_name: str | None = None) -> bool:
    """Checks for valid credentials"""

    session = boto3.Session(profile_name=profile_name)

    try:
        # Attempt to get identity
        sts = session.client("sts")
        # Suppress boto3 printing stack trace when credential expires
        with redirect_stderr(io.StringIO()):
            sts.get_caller_identity()
        return True
    except (
        NoCredentialsError,
        SSOTokenLoadError,
        TokenRetrievalError,
        UnauthorizedSSOTokenError,
    ):
        return False
    except ClientError as e:
        if e.response["Error"]["Code"] in ["ExpiredToken", "ExpiredTokenException"]:
            # Temporary IAM credentials have expired.
            return False
        else:
            # It's a different AWS error (e.g., AccessDenied), so re-raise it
            raise e


def refresh_credentials(profile_name: str | None = None):
    # Construct the login command
    login_cmd = ["aws", "sso", "login"]
    if profile_name:
        login_cmd.extend(["--profile", profile_name])

    try:
        # Run the AWS CLI SSO login command
        subprocess.run(login_cmd, check=True)
        logger.info("Login successful.")
        return True

    except subprocess.CalledProcessError:
        logger.error("Failed to login via AWS SSO.")
        sys.exit(1)


def create_ce_client(
    *,
    client_name: str = "ce",
    profile_name: str | None = None,
    region: str = "us-east-1",
):
    if not check_aws_auth(profile_name):
        refresh_credentials(profile_name)
    session = boto3.Session(profile_name=profile_name)
    return session.client(client_name, region_name=region)
