# Бизнес-логика для генерации транзакций (MCMC / Metropolis)
ACTIVITY_DISTRIBUTION = [
    0.05, 0.03, 0.02, 0.01, 0.01, 0.02,
    0.10, 0.25, 0.55, 0.85, 0.90, 0.80,
    0.65, 0.55, 0.50, 0.55, 0.65, 0.75,
    0.85, 0.70, 0.50, 0.35, 0.20, 0.10
]
ITERATIONS_QUANTITY = 8000

# Настройки Faker для локаций
LOCATIONS = [
    'az_AZ', 'bg_BG', 'bn_BD', 'cs_CZ', 'da_DK', 'de_AT', 'de_CH', 'de_DE',
    'el_GR', 'en_PH', 'en_US', 'es_CL', 'es_ES', 'es_MX', 'fa_IR', 'fi_FI',
    'fil_PH', 'fr_CH', 'fr_FR', 'hr_HR', 'hu_HU', 'hy_AM', 'id_ID', 'it_IT',
    'ja_JP', 'ko_KR', 'nl_BE', 'nl_NL', 'no_NO', 'pl_PL', 'pt_BR', 'pt_PT',
    'ro_RO', 'ru_RU', 'sk_SK', 'sl_SI', 'sv_SE', 'th_TH', 'tl_PH', 'tr_TR',
    'vi_VN', 'zh_CN', 'zh_TW'
]

# Профили клиентов для Startpack
CUSTOMER_CATEGORIES = {
    'organic': {
        'name': 'Organic Search',
        'prob': 0.4,
        'avg_initial_deposit': 150.0,
        'churn_risk': 'Low',
        'lifetime_value': 'High'
    },
    'referral': {
        'name': 'Referral Program',
        'prob': 0.3,
        'avg_initial_deposit': 250.0,
        'churn_risk': 'Medium',
        'lifetime_value': 'Medium'
    },
    'paid_ads': {
        'name': 'Paid Advertising',
        'prob': 0.3,
        'avg_initial_deposit': 100.0,
        'churn_risk': 'High',
        'lifetime_value': 'Low'
    }
}

# Банковские настройки
CURRENCIES = {'GBP': 0.70, 'EUR': 0.20, 'USD': 0.10}
ACCOUNT_TYPES = {'Personal': 0.7, 'Business': 0.15, 'Premium': 0.10, 'Metal': 0.05}

BANK_NAMES = [
    'Barclays', 'HSBC', 'Lloyds', 'NatWest', 'Santander',
    'Nationwide', 'RBS', 'Standard Chartered', 'Monzo', 'Starling'
]

# Параметры экстракции
BUCKET_RAW = 'raw'
NUM_ACCOUNTS_STARTPACK = 500

DOMAINS = ["gmail.com", "outlook.com", "yahoo.com", "revolut.com", "protonmail.com"]
MIN_AGE_CLIENT = 18
MAX_AGE_CLIENT = 75

DOMAINS = ["gmail.com", "outlook.com", "revolut.com"]

INITIAL_MERCHANTS = [
    "Starbucks", "Amazon", "Apple", "Uber", "Netflix", 
    "Tesco", "Sainsbury's", "McDonald's", "Zara"
]