import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import jwt
import requests


KID = str(os.getenv("REVOLUT_KID"))
REDIRECT_URL = os.getenv("REVOLUT_REDIRECT_URL")


class RevolutClient:
    def __init__(self):
        self.client_id = os.getenv("REVOLUT_CLIENT_ID")
        self.financial_id = os.getenv("REVOLUT_FINANCIAL_ID")

        project_root = Path(__file__).resolve().parent.parent.parent
        local_certs = project_root / "certs"

        # env_private = os.getenv("REVOLUT_PRIVATE_KEY_PATH")
        # env_transport = os.getenv("REVOLUT_TRANSPORT_CERT_PATH")

        self.private_key_path = local_certs / "private.key"
        self.transport_cert_path = local_certs / "transport.pem"

        self.signing_cert_path = Path(
            os.getenv("REVOLUT_SIGNING_CERT_PATH", local_certs / "signing.pem")
        )

        self.base_api = "https://sandbox-oba-auth.revolut.com"
        self.auth_url = "https://sandbox-oba-auth.revolut.com/token"
        self.ui_url = "https://sandbox-oba.revolut.com/ui/index.html"

        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expires_at: float = 0

    def _cert(self):
        """MTLS (transport)"""
        return (str(self.transport_cert_path), str(self.private_key_path))

    def _get_signing_key(self):
        """Читаем private key для подписи JWT (request object)"""
        with open(self.private_key_path, "rb") as f:
            return f.read()

    def _get_client_credentials_token(self) -> str:
        """Только для создания consent"""
        resp = requests.post(
            self.auth_url,
            cert=self._cert(),
            data={
                "grant_type": "client_credentials",
                "scope": "accounts",
                "client_id": self.client_id,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            verify=False,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    def create_consent(self) -> Dict:
        """Создаёт Account Access Consent"""
        token = self._get_client_credentials_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "x-fapi-financial-id": self.financial_id,
            "x-fapi-interaction-id": str(uuid.uuid4()),
            "x-idempotency-key": str(uuid.uuid4()),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        payload = {
            "Data": {
                "Permissions": [
                    "ReadAccountsBasic",
                    "ReadAccountsDetail",
                    "ReadTransactionsBasic",
                    "ReadTransactionsDetail",
                    "ReadTransactionsCredits",
                    "ReadTransactionsDebits",
                ]
            },
            "Risk": {}
        }

        resp = requests.post(
            f"{self.base_api}/account-access-consents",
            json=payload,
            cert=self._cert(),
            headers=headers,
            verify=False,
        )

        resp.raise_for_status()
        return resp.json()

    def get_authorization_url(
            self,
            consent_id: str,
            redirect_uri: str = REDIRECT_URL
    ) -> str:
        now = int(time.time())
        claims = {
            "iss": self.client_id,
            "aud": "https://oba-auth.revolut.com",
            "response_type": "code id_token",
            "client_id": self.client_id,
            "redirect_uri": redirect_uri,
            "scope": "accounts",
            "state": str(uuid.uuid4()),
            "nonce": str(uuid.uuid4()),
            "max_age": 3600,
            "nbf": now,
            "exp": now + 300,
            "claims": {
                "id_token": {
                    "openbanking_intent_id": {
                        "value": consent_id,
                        "essential": True
                        }
                }
            }
        }

        private_key = self._get_signing_key()
        signed_jwt = jwt.encode(
            claims,
            private_key,
            algorithm="PS256",
            headers={
                "kid": KID,
                "alg": "PS256",
                "typ": "JWT"
            }
        )

        params = {
            "response_type": "code id_token",
            "client_id": self.client_id,
            "redirect_uri": redirect_uri,
            "scope": "accounts",
            "request": signed_jwt,
            "nonce": claims["nonce"],
            "state": claims["state"]
        }

        url = self.ui_url + "?" + "&".join(
            f"{k}={v}" for k, v in params.items()
        )
        return url

    def exchange_code(self, authorization_code: str) -> Dict:
        resp = requests.post(
            self.auth_url,
            cert=self._cert(),
            data={
                "grant_type": "authorization_code",
                "client_id": self.client_id,
                "code": authorization_code,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            verify=False,
        )
        resp.raise_for_status()
        td = resp.json()

        self.access_token = td["access_token"]
        self.refresh_token = td.get("refresh_token")
        self.token_expires_at = time.time() + td.get("expires_in", 300) - 60

        print("Tokens received (access + refresh)")
        return td

    def refresh_tokens(self):
        if not self.refresh_token:
            raise Exception("No refresh_token. Run full consent flow again.")

        resp = requests.post(
            self.auth_url,
            cert=self._cert(),
            data={
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "refresh_token": self.refresh_token,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            verify=False,
        )
        resp.raise_for_status()
        td = resp.json()

        self.access_token = td["access_token"]
        if "refresh_token" in td:
            self.refresh_token = td["refresh_token"]
        self.token_expires_at = time.time() + td.get("expires_in", 300) - 60

    def _ensure_valid_token(self):
        if not self.access_token or time.time() > self.token_expires_at:
            self.refresh_tokens()

    def _build_headers(self) -> Dict:
        self._ensure_valid_token()
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "x-fapi-financial-id": self.financial_id,
            "x-fapi-interaction-id": (
                f"int-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:-3]}"
            ),
        }

    def get_accounts(self) -> Dict:
        headers = self._build_headers()
        resp = requests.get(
            f"{self.base_api}/accounts",
            cert=self._cert(),
            headers=headers,
            verify=False,
        )
        if resp.status_code == 401:
            self.refresh_tokens()
            headers = self._build_headers()
            resp = requests.get(
                f"{self.base_api}/accounts",
                cert=self._cert(),
                headers=headers,
                verify=False,
            )
        resp.raise_for_status()
        return resp.json()

    def get_transactions(
            self,
            account_id: str,
            from_date: Optional[str] = None,
            to_date: Optional[str] = None
    ) -> Dict:
        headers = self._build_headers()
        params = {}
        if from_date:
            params["fromBookingDateTime"] = from_date
        if to_date:
            params["toBookingDateTime"] = to_date

        resp = requests.get(
            f"{self.base_api}/accounts/{account_id}/transactions",
            cert=self._cert(),
            headers=headers,
            params=params,
            verify=False,
        )
        if resp.status_code == 401:
            self.refresh_tokens()
            headers = self._build_headers()
            resp = requests.get(
                f"{self.base_api}/accounts/{account_id}/transactions",
                cert=self._cert(), headers=headers, params=params,
                verify=False,
            )
        resp.raise_for_status()
        return resp.json()
