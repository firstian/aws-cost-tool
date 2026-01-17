import subprocess
from unittest.mock import MagicMock, Mock, patch

import pytest
from botocore.exceptions import (
    ClientError,
    NoCredentialsError,
    SSOTokenLoadError,
    TokenRetrievalError,
    UnauthorizedSSOTokenError,
)

from aws_cost_tool.client import check_aws_auth, create_ce_client, refresh_credentials


class TestCheckAwsAuth:
    """Tests for check_aws_auth function."""

    @patch("aws_cost_tool.client.boto3.Session")
    def test_successful_authentication(self, mock_session, capsys):
        """Test successful authentication prints the ARN."""
        mock_sts = Mock()
        mock_sts.get_caller_identity.return_value = {
            "Arn": "arn:aws:iam::123456789012:user/testuser"
        }
        mock_session.return_value.client.return_value = mock_sts

        assert check_aws_auth()
        mock_session.assert_called_once_with(profile_name=None)
        mock_sts.get_caller_identity.assert_called_once()

    @patch("aws_cost_tool.client.boto3.Session")
    def test_successful_authentication_with_profile(self, mock_session, capsys):
        """Test successful authentication with profile name."""
        mock_sts = Mock()
        mock_sts.get_caller_identity.return_value = {
            "Arn": "arn:aws:iam::123456789012:user/testuser"
        }
        mock_session.return_value.client.return_value = mock_sts

        assert check_aws_auth(profile_name="my-profile")

        mock_session.assert_called_once_with(profile_name="my-profile")

    @patch("aws_cost_tool.client.boto3.Session")
    def test_no_credentials_error(self, mock_session):
        """Test NoCredentialsError"""
        mock_sts = Mock()
        mock_sts.get_caller_identity.side_effect = NoCredentialsError()
        mock_session.return_value.client.return_value = mock_sts

        assert not check_aws_auth()

    @patch("aws_cost_tool.client.boto3.Session")
    def test_sso_token_load_error(self, mock_session):
        """Test SSOTokenLoadError"""
        mock_sts = Mock()
        mock_sts.get_caller_identity.side_effect = SSOTokenLoadError(
            error_msg="SSO token error"
        )
        mock_session.return_value.client.return_value = mock_sts

        assert not check_aws_auth(profile_name="test-profile")

    @patch("aws_cost_tool.client.boto3.Session")
    def test_token_retrieval_error(self, mock_session):
        """Test TokenRetrievalError"""
        mock_sts = Mock()
        mock_sts.get_caller_identity.side_effect = TokenRetrievalError(
            provider="test", error_msg="Token error"
        )
        mock_session.return_value.client.return_value = mock_sts
        assert not check_aws_auth()

    @patch("aws_cost_tool.client.boto3.Session")
    def test_unauthorized_sso_token_error(self, mock_session):
        """Test UnauthorizedSSOTokenError"""
        mock_sts = Mock()
        mock_sts.get_caller_identity.side_effect = UnauthorizedSSOTokenError(
            error_msg="Unauthorized SSO token"
        )
        mock_session.return_value.client.return_value = mock_sts
        assert not check_aws_auth()

    @patch("aws_cost_tool.client.boto3.Session")
    def test_expired_token_error(self, mock_session):
        """Test ExpiredToken ClientError"""
        mock_sts = Mock()
        error_response = {"Error": {"Code": "ExpiredToken", "Message": "Token expired"}}
        mock_sts.get_caller_identity.side_effect = ClientError(
            error_response, "GetCallerIdentity"
        )
        mock_session.return_value.client.return_value = mock_sts
        assert not check_aws_auth()

    @patch("aws_cost_tool.client.boto3.Session")
    def test_expired_token_exception(self, mock_session):
        """Test ExpiredTokenException ClientError"""
        mock_sts = Mock()
        error_response = {
            "Error": {"Code": "ExpiredTokenException", "Message": "Token expired"}
        }
        mock_sts.get_caller_identity.side_effect = ClientError(
            error_response, "GetCallerIdentity"
        )
        mock_session.return_value.client.return_value = mock_sts
        assert not check_aws_auth()

    @patch("aws_cost_tool.client.boto3.Session")
    def test_other_client_error_raises(self, mock_session):
        """Test that non-auth related ClientErrors are re-raised."""
        mock_sts = Mock()
        error_response = {"Error": {"Code": "AccessDenied", "Message": "Access denied"}}
        mock_sts.get_caller_identity.side_effect = ClientError(
            error_response, "GetCallerIdentity"
        )
        mock_session.return_value.client.return_value = mock_sts

        with pytest.raises(ClientError) as exc_info:
            check_aws_auth()

        assert exc_info.value.response["Error"]["Code"] == "AccessDenied"


class TestRefreshCredentials:
    """Tests for refresh_credentials function."""

    @patch("aws_cost_tool.client.subprocess.run")
    def test_successful_login_without_profile(self, mock_run, capsys):
        """Test successful SSO login without profile."""
        mock_run.return_value = MagicMock(returncode=0)

        result = refresh_credentials()

        mock_run.assert_called_once_with(["aws", "sso", "login"], check=True)
        assert result is True

    @patch("aws_cost_tool.client.subprocess.run")
    def test_successful_login_with_profile(self, mock_run, capsys):
        """Test successful SSO login with profile."""
        mock_run.return_value = MagicMock(returncode=0)

        result = refresh_credentials(profile_name="my-profile")

        mock_run.assert_called_once_with(
            ["aws", "sso", "login", "--profile", "my-profile"], check=True
        )
        assert result is True

    @patch("aws_cost_tool.client.subprocess.run")
    def test_failed_login_exits(self, mock_run, capsys):
        """Test that failed login exits the program."""
        mock_run.side_effect = subprocess.CalledProcessError(1, "aws sso login")

        with pytest.raises(SystemExit) as exc_info:
            refresh_credentials()

        assert exc_info.value.code == 1

    @patch("aws_cost_tool.client.subprocess.run")
    def test_failed_login_with_profile_exits(self, mock_run):
        """Test that failed login with profile exits the program."""
        mock_run.side_effect = subprocess.CalledProcessError(1, "aws sso login")

        with pytest.raises(SystemExit) as exc_info:
            refresh_credentials(profile_name="my-profile")

        assert exc_info.value.code == 1
        mock_run.assert_called_once_with(
            ["aws", "sso", "login", "--profile", "my-profile"], check=True
        )


class TestCreateCeClient:
    """Tests for create_ce_client function."""

    @patch("aws_cost_tool.client.check_aws_auth")
    @patch("aws_cost_tool.client.boto3.Session")
    def test_create_client_default_region(self, mock_session, mock_check_auth):
        """Test creating CE client with default region."""
        mock_ce_client = Mock()
        mock_session.return_value.client.return_value = mock_ce_client

        result = create_ce_client()

        mock_check_auth.assert_called_once_with(None)
        mock_session.assert_called_once_with(profile_name=None)
        mock_session.return_value.client.assert_called_once_with(
            "ce", region_name="us-east-1"
        )
        assert result == mock_ce_client

    @patch("aws_cost_tool.client.check_aws_auth")
    @patch("aws_cost_tool.client.boto3.Session")
    def test_create_client_with_profile(self, mock_session, mock_check_auth):
        """Test creating CE client with profile name."""
        mock_ce_client = Mock()
        mock_session.return_value.client.return_value = mock_ce_client

        result = create_ce_client(profile_name="my-profile")

        mock_check_auth.assert_called_once_with("my-profile")
        mock_session.assert_called_once_with(profile_name="my-profile")
        assert result == mock_ce_client

    @patch("aws_cost_tool.client.check_aws_auth")
    @patch("aws_cost_tool.client.boto3.Session")
    def test_create_client_custom_region(self, mock_session, mock_check_auth):
        """Test creating CE client with custom region."""
        mock_ce_client = Mock()
        mock_session.return_value.client.return_value = mock_ce_client

        result = create_ce_client(region="eu-west-1")

        mock_check_auth.assert_called_once_with(None)
        mock_session.return_value.client.assert_called_once_with(
            "ce", region_name="eu-west-1"
        )
        assert result == mock_ce_client

    @patch("aws_cost_tool.client.check_aws_auth")
    @patch("aws_cost_tool.client.boto3.Session")
    def test_create_client_with_profile_and_region(self, mock_session, mock_check_auth):
        """Test creating CE client with both profile and custom region."""
        mock_ce_client = Mock()
        mock_session.return_value.client.return_value = mock_ce_client

        result = create_ce_client(profile_name="my-profile", region="ap-south-1")

        mock_check_auth.assert_called_once_with("my-profile")
        mock_session.assert_called_once_with(profile_name="my-profile")
        mock_session.return_value.client.assert_called_once_with(
            "ce", region_name="ap-south-1"
        )
        assert result == mock_ce_client
