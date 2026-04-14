import psycopg2
import random
import uuid
from datetime import datetime
from faker import Faker
from constants.constants import (
    LOCATIONS,
    NUM_ACCOUNTS,
    CURRENCIES,
    ACCOUNT_TYPES
)

fake = Faker(random.choice(LOCATIONS))


def get_currency():
    currencies = list(CURRENCIES.keys())
    weights = list(CURRENCIES.values())

    if sum(weights) != 1:
        sum_weight = sum(weights)
        norm_weights = [weight / sum_weight for weight in weights]
        return random.choice(currencies, weights=norm_weights)[0]
    return random.choice(currencies, weights=weights)[0]


def get_account_type():
    account_type = list(ACCOUNT_TYPES.keys())
    weights = list(ACCOUNT_TYPES.values())

    if sum(weights) != 1:
        sum_weight = sum(weights)
        norm_weights = [weight / sum_weight for weight in weights]
        return random.choice(account_type, weights=norm_weights)[0]
    return random.choice(account_type, weights=weights)[0]


def generate_account_id():
    return str(uuid.uuid4()).replace('-', '')[:32]


def generate_iban(account_id):
    numeric_part = ''.join([str(ord(c) % 10) for c in account_id[:20]])
    bban = numeric_part.ljust(22, '0')[:22]
    return f"GB{random.randint(10, 99)}{bban}"


def generate_sort_code():
    return (
        f"{random.randint(1, 99):02d}"
        f"-{random.randint(1, 99):02d}"
        f"-{random.randint(1, 99):02d}"
    )


def generate_account_number():
    return f"{random.randint(10000000, 99999999)}"


def generate_name():
    return f"{fake.first_name()} {fake.last_name()}"


def generate_account(account_id, currency, account_type, account_sub_type):
    iban = generate_iban(account_id)
    sort_code = generate_sort_code()
    account_number = generate_account_number()
    name = generate_name()

    account_data = {
        "AccountId": account_id,
        "Currency": currency,
        "AccountType": account_type,
        "AccountSubType": account_sub_type,
        "Account": [
            {
                "SchemeName": "UK.OBIE.IBAN",
                "Identification": iban,
                "Name": name
            },
            {
                "SchemeName": "UK.OBIE.SortCodeAccountNumber",
                "Identification": f"{sort_code}/{account_number}",
                "Name": name
            }
        ]
    }

    return account_data


def generate_accounts_json_format(num_accounts):
    """Генерирует список аккаунтов в формате, совместимом с Revolut"""
    accounts = []

    for _ in range(num_accounts):
        account_id = generate_account_id()
        currency = get_currency()
        account_type = get_account_type()
        account_sub_type = (
            'CurrentAccount' if account_type == 'Personal' else account_type
        )

        account = generate_account(
            account_id, currency, account_type, account_sub_type
        )
        accounts.append(account)

    revolut_format = {
        "Data": {
            "Account": accounts
        },
        "Links": {
            "Self": "https://sandbox-oba-auth.revolut.com/accounts"
        },
        "Meta": {
            "TotalPages": 1
        }
    }

    return revolut_format


def insert_into_postgres(accounts_data):
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="postgres",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS silver")

    inserted = 0
    skipped = 0

    for account in accounts_data["Data"]["Account"]:
        try:
            cur.execute("""
                INSERT INTO silver.dim_accounts (
                    account_id,
                    currency,
                    account_type,
                    account_sub_type,
                    inserted_at
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (account_id) DO NOTHING
            """, (
                account["AccountId"],
                account["Currency"],
                account["AccountType"],
                account["AccountSubType"],
                datetime.now()
            ))

            if cur.rowcount > 0:
                inserted += 1
            else:
                skipped += 1

        except Exception as e:
            print(f"ERROR {account['AccountId']}: {e}")

    conn.commit()
    cur.close()
    conn.close()

    return inserted, skipped


def save_to_json(accounts_data, filename="generated_accounts.json"):
    import json
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(accounts_data, f, indent=2, ensure_ascii=False)
    print(f"JSON saved in {filename}")


def main():
    accounts_data = generate_accounts_json_format(NUM_ACCOUNTS)

    save_to_json(
        accounts_data,
        f"accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )


if __name__ == "__main__":
    main()
