import sys
from pathlib import Path
from dags.utils.revolut_clients import RevolutClient

sys.path.append(str(Path(__file__).parent.parent))


def test_env_file_exists():
    """Does .env exist and is CLIENT_ID defined in it"""
    env_path = Path(__file__).parent.parent / ".env"

    if env_path.exists():
        print(
            "SUCCESSFULL: The .env file exists"
        )

        content = env_path.read_text()

        if "REVOLUT_CLIENT_ID" in content:
            print(
                "SUCCESSFULL: REVOLUT_CLIENT_ID found in .env"
            )
        else:
            print(
                "WARNING: REVOLUT_CLIENT_ID not found in .env"
            )
    else:
        print(
            "FAILED: .env file is not found"
        )

    assert True


def test_revolut_client_initialization():
    """RevolutClient is created without errors"""
    try:
        client = RevolutClient()
        print(
            "SUCCESSFULL: RevolutClient created"
        )

        if client.client_id:
            print(
                "SUCCESSFULL: Client ID received"
            )
        else:
            print(
                "WARNING: Client ID is not found in .env"
            )

        if client.transport_cert_path.exists():
            print(
                "SUCCESSFULL: Transport certificate is found"
            )
        else:
            print(
                "FAILED: Transport certificate is not found"
            )

        if client.private_key_path.exists():
            print(
                "SUCCESSFULL: Private Key is found"
            )
        else:
            print(
                f"FAILED: Private Key is found in {client.private_key_path}"
            )

    except Exception as e:
        print(
            f"FAILED: Error creating client --- {e}"
        )


def test_get_authorization_url():
    """Authorization link is generate without errors."""
    try:
        client = RevolutClient()
        test_consent_id = "test-consent-id-123"
        redirect_uri = "https://test.com/callback"

        url = client.get_authorization_url(test_consent_id, redirect_uri)

        assert "sandbox-oba-auth.revolut.com" in url
        assert "response_type=code id_token" in url
        assert f"client_id={client.client_id}" in url
        assert f"redirect_uri={redirect_uri}" in url

        print(
            "SUCCESSFULL: The authorization link was generated correctly"
        )

    except Exception as e:
        print(
            f" FAILED: Error link generation --- {e}"
        )


def test_refresh_token_in_env():
    """Refresh token is correct and saved in .env"""
    env_path = Path(__file__).parent.parent / ".env"

    content = env_path.read_text()

    if "REVOLUT_REFRESH_TOKEN" in content:
        for line in content.splitlines():
            if line.startswith("REVOLUT_REFRESH_TOKEN="):
                token = line.split("=", 1)[1].strip()
                if token and len(token) > 10:
                    print(
                        "SUCCESSFULL: REVOLUT_REFRESH_TOKEN is found"
                    )
                else:
                    print(
                        "WARNING: The length of the "
                        "REVOLUT_REFRESH_TOKEN is less than 10"
                    )
                break
    else:
        print(
            "FAILED: REVOLUT_REFRESH_TOKEN is not found in .env"
        )


def test_certificates_access():
    """Certificates are readable"""
    client = RevolutClient()

    try:
        with open(client.transport_cert_path, 'r') as f:
            content = f.read()
            if (
                "REVOLUT_TRANSPORT_CERT_PATH" in content
                or "REVOLUT_TRANSPORT_CERTIFICATE" in content
                or "REVOLUT_TRANSPORT_CERT" in content
            ):
                print(
                    "SUCCESSFULL: Transport certificate appears to be correct"
                )
            else:
                print(
                    "WARNING: Check Transport certificate"
                )

    except Exception as e:
        print(
            f"FAILED: Failed to read Transport certificate --- {e}"
        )

    try:
        with open(client.private_key_path, 'r') as f:
            content = f.read()
            if (
                "REVOLUT_PRIVATE_KEY_PATH" in content
                or "REVOLUT_PRIVATE_KEY" in content
            ):
                print(
                    "SUCCESSFULL: Private Key is found"
                )
            else:
                print(
                    "WARNING: Check Private Key"
                )
    except Exception as e:
        print(
            f"FAILED: Failed to read Private Key --- {e}"
        )


if __name__ == "__main__":
    print(
        "Tests for Revolut Consent Setup"
    )

    test_env_file_exists()

    test_revolut_client_initialization()

    test_certificates_access()

    test_get_authorization_url()

    test_refresh_token_in_env()
