import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent / "dags"))

from utils.revolut_clients import RevolutClient

env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)


def main():
    client = RevolutClient()

    if not client.client_id:
        raise ValueError("REVOLUT_CLIENT_ID not found in .env")

    consent = client.create_consent()
    consent_id = consent["Data"]["ConsentId"]

    redirect_uri = os.getenv("REVOLUT_REDIRECT_URL")
    url = client.get_authorization_url(consent_id, redirect_uri)

    print(url)

    code = input("Enter authorization code: ").strip()

    if not code:
        raise ValueError("Authorization code is required")

    client.exchange_code(code)

    refresh_token = client.refresh_token
    env_path = Path(".env")

    if env_path.exists():
        content = env_path.read_text(encoding="utf-8")
        if "REVOLUT_REFRESH_TOKEN" in content:
            lines = [
                line if not line.startswith("REVOLUT_REFRESH_TOKEN=")
                else f"REVOLUT_REFRESH_TOKEN={refresh_token}"
                for line in content.splitlines()
            ]
            content = "\n".join(lines)
        else:
            content += f"\nREVOLUT_REFRESH_TOKEN={refresh_token}\n"
        env_path.write_text(content, encoding="utf-8")
    else:
        env_path.write_text(
            f"REVOLUT_REFRESH_TOKEN={refresh_token}\n", encoding="utf-8"
        )


if __name__ == "__main__":
    main()
