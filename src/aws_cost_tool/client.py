import subprocess
import sys

import boto3
from botocore.exceptions import (
    ClientError,
    NoCredentialsError,
    SSOTokenLoadError,
    TokenRetrievalError,
    UnauthorizedSSOTokenError,
)


def check_aws_auth(profile_name: str | None = None) -> None:
    """Checks for valid credentials and triggers SSO login if needed."""

    session = boto3.Session(profile_name=profile_name)
    sts = session.client("sts")

    try:
        # Attempt to get identity
        identity = sts.get_caller_identity()
        print(f"Authenticated as: {identity['Arn']}")

    except (
        NoCredentialsError,
        SSOTokenLoadError,
        TokenRetrievalError,
        UnauthorizedSSOTokenError,
    ):
        print("AWS credentials expired or not found. Attempting SSO login...")
        refresh_credentials(profile_name)
    except ClientError as e:
        if e.response["Error"]["Code"] in ["ExpiredToken", "ExpiredTokenException"]:
            print("Temporary IAM credentials have expired.")
            refresh_credentials(profile_name)
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
        print("Login successful.")
        return True

    except subprocess.CalledProcessError:
        print("Failed to login via AWS SSO.", file=sys.stderr)
        sys.exit(1)


def create_ce_client(
    *,
    client_name: str = "ce",
    profile_name: str | None = None,
    region: str = "us-east-1",
):
    check_aws_auth(profile_name)
    session = boto3.Session(profile_name=profile_name)
    return session.client(client_name, region_name=region)
