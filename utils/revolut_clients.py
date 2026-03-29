import requests as req
# import json
from pathlib import Path
from datetime import datetime as dt
import os

class RevolutClient:
    def __init__(self):
        self.client_id = os.getenv("REVOLUT_CLIENT_ID")
        self.privat_key_path = Path(os.getenv('REVOLUT_PRIVATE_KEY_PATH', 'secret/private.key'))
        self.transport_cert_path = Path(os.getenv('REVOLUT_TRANSPORT_CERT_PATH', 'certs/transport.pem'))
        self.token = None
        self.token_expires = None
    
    def get_access_token(self):
        if (
            self.token and
            self.token_expires and
            self.token_expires > dt.now().timestamp()
        ):
            return self.token

        token_url = "https://sandbox-oba-auth.revolut.com/token"

        with (
            open(self.privat_key_path, 'rb') as pr_key,
            open(self.transport_cert_path, 'rb') as tr_cert
        ):
            response = req.post(
                token_url,
                cert=(tr_cert.name, pr_key.name),
                data={
                    "grant_type": 'client_credentials',
                    "scope": 'accounts',
                    "client_id": self.client_id,
                },
                headers={"Content-Type": 'application/x-www-form-urlencoded'},
                verify=False
            )
        
        if response.ok:
            data = response.json()
            self.token - data['access_token']
            self.token_expires = dt.now().timestamp() + data.get('expires_in', 3600) - 60
            print('Success get new token')
            return self.token
        else:
            raise Exception(f'Failed to get token: {response.text}')
    
    def get_accounts(self):
        token = self.get_access_token()
        url = "https://sandbox-oba.revolut.com/accounts"

        headers = {
            "Authorization": f"Bearer {token}",
        }

        response = req.get(
            url,
            headers=headers,
            cert=(self.transport_cert_path, self.private_key_path),
            verify=False
        )

        if response.ok:
            return response.json()
        else:
            raise Exception(f"Failed to get accounts: status code {response.status_code}, {response.text}")
    
    def get_transactions(self, account_id: str, from_date=None, to_date=None):
        token = self.get_access_token()
        url = f"https://sandbox-oba.revolut.com/accounts/{account_id}/transactions"
        
        params = {}
        if from_date:
            params["fromBookingDateTime"] = from_date
        if to_date:
            params["toBookingDateTime"] = to_date

        headers = {
            "Authorization": f"Bearer {token}",
            "x-fapi-financial-id": "001580000103UAvAAM",
        }

        response = req.get(
            url, 
            headers=headers, 
            params=params,
            cert=(self.transport_cert_path, self.private_key_path), 
            verify=False
        )
        
        if response.ok:
            return response.json()
        else:
            raise Exception(f"Failed to get transactions for {account_id}: {response.text}")
