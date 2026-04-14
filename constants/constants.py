"""Generate transactions DAG"""

# Activity distribution over time

ACTIVITY_DISTRIBUTION = [
    0.05, 0.03, 0.02, 0.01, 0.01, 0.02,
    0.10, 0.25, 0.55, 0.85, 0.90, 0.80,
    0.65, 0.55, 0.50, 0.55, 0.65, 0.75,
    0.85, 0.70, 0.50, 0.35, 0.20, 0.10
]

# Quantity of iterations Metropolis step

ITERATIONS_QUANTITY = 8000

# Locations for Faker

LOCATIONS = [
    'az_AZ', 'bg_BG', 'bn_BD', 'cs_CZ', 'da_DK', 'de_AT', 'de_CH', 'de_DE',
    'el_GR', 'en_PH', 'en_US', 'es_CL', 'es_ES', 'es_MX', 'fa_IR', 'fi_FI',
    'fil_PH', 'fr_CH', 'fr_FR', 'hr_HR', 'hu_HU', 'hy_AM', 'id_ID', 'it_IT',
    'ja_JP', 'ko_KR', 'nl_BE', 'nl_NL', 'no_NO', 'pl_PL', 'pt_BR', 'pt_PT',
    'ro_RO', 'ru_RU', 'sk_SK', 'sl_SI', 'sv_SE', 'th_TH', 'tl_PH', 'tr_TR',
    'vi_VN', 'zh_CN', 'zh_TW'
]

"""Generate accounts DAG"""

# Categories of new clients

CUSTOMER_CATEGORIES = {
    'referral_friend': {
        'name': 'Came on the recommendation of a friend',
        'prob': 0.25,
        'avg_initial_deposit': 500,
        'churn_risk': 'low',
        'lifetime_value': 'high'
    },
    'advertisement': {
        'name': 'Saw an advertisement',
        'prob': 0.20,
        'avg_initial_deposit': 200,
        'churn_risk': 'medium',
        'lifetime_value': 'medium'
    },
    'promotion': {
        'name': 'liked the promotion for the product',
        'prob': 0.15,
        'avg_initial_deposit': 1000,
        'churn_risk': 'low',
        'lifetime_value': 'high'
    },
    'call_center': {
        'name': 'The operator called',
        'prob': 0.10,
        'avg_initial_deposit': 150,
        'churn_risk': 'medium',
        'lifetime_value': 'medium'
    },
    'organic': {
        'name': 'Registered by myself',
        'prob': 0.20,
        'avg_initial_deposit': 300,
        'churn_risk': 'medium',
        'lifetime_value': 'medium'
    },
    'partner': {
        'name': 'Came from a partner',
        'prob': 0.10,
        'avg_initial_deposit': 750,
        'churn_risk': 'low',
        'lifetime_value': 'high'
    }
}

# Currencies and their probability

CURRENCIES = {
    'GPB': 0.70,
    'EUR': 0.20,
    'USD': 0.10,
}

# Accounts type and their probability

ACCOUNT_TYPES = {
    'Personal': 0.7,
    'Business': 0.15,
    'Premium': 0.10,
    'Metal': 0.05,
}

# Merchants for accounts

INITIAL_MERCHANTS = [
    'Welcome Bonus', 'Account Opening', 'Initial Deposit', 'First Top-up',
    'Referral Reward', 'Sign-up Promotion', 'Welcome Gift'
]

# Email domains

DOMAINS = [
            'gmail.com', 'yahoo.com',
            'hotmail.com', 'outlook.com',
            'icloud.com', 'proton.me'
        ]

# Age MIN MAX

MIN_AGE_CLIENT = 18
MAX_AGE_CLIENT = 80


"""Extract accounts and transactions DAG"""

BUCKET_RAW = 'raw'

"""Load startpack to silver script"""

# Start quantity of accounts to need generate

NUM_ACCOUNTS = 500

# Schemes

SCHEMES = [
    'UK.OBIE.IBAN',
    'UK.OBIE.SortCodeAccountNumber'
]

# Bank names

BANK_NAMES = [
    'Barclays', 'HSBC', 'Lloyds', 'NatWest', 'Santander',
    'Nationwide', 'RBS', 'Standard Chartered', 'Monzo', 'Starling'
]
