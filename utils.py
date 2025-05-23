import psycopg2.pool
import hashlib
import logging
import psycopg2
from dotenv import load_dotenv
import os


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='banking_system.log'
)
logger = logging.getLogger('banking_utils')
'''
Database functions

'''
load_dotenv()
connection_pool = None

def init_connection_pool(min_conn=1, max_conn=10):
    '''Initialize the database connection pool'''
    global connection_pool

    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            min_conn,
            max_conn,
            host = os.getenv("HOST"),
            database = os.getenv("DBNAME"),
            user = os.getenv("USER"),
            password = os.getenv("PASSWORD"),
            port = os.getenv("PORT")
        )
        logger.info("Connection pool created successfully")
    except Exception as e:
        logger.error(f"Error creating connection pool: {e}")
        raise

def connect_to_db():
    '''Gets a connection from the pool'''
    global connection_pool

    # Initialize pool if not already done
    if connection_pool is None:
        init_connection_pool()

    try:
        conn = connection_pool.getconn()
        conn.autocommit = False
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def release_conn(conn):
    '''Returns a connection to the pool'''
    global connection_pool
    if connection_pool and conn:
        connection_pool.putconn(conn)

def create_tables():
    conn = connect_to_db()

    try:

        with conn.cursor() as cur:
            '''CREATE Tables if they don't exist'''

            # User Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Users (
                    user_id SERIAL PRIMARY KEY, 
                    username varchar(50), 
                    password varchar(64), 
                    email varchar(50), 
                    fullname varchar(50), 
                    created_on TIMESTAMP, 
                    is_admin BOOL,
                    failed_login_attempts INTEGER,
                    last_login TIMESTAMP
                );""")

            # Accounts Table   
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Accounts (
                    user_id integer NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE, 
                    account_id SERIAL PRIMARY KEY, 
                    currency_code varchar(3) NOT NULL REFERENCES Currencies(currency_code), 
                    balance DECIMAL(15,2) NOT NULL DEFAULT 0.00, 
                    created_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
                    is_active BOOLEAN DEFAULT TRUE,
                    UNIQUE(user_id, currency_code)        
                );""")
        
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Transactions (
                    tx_time TIMESTAMP, 
                    tx_id varchar(50),
                    type varchar(50), 
                    from_user_id integer REFERENCES Users(user_id),
                    from_account_id integer REFERENCES Accounts(account_id), 
                    to_user_id integer REFERENCES Users(user_id), 
                    to_account_id integer REFERENCES Accounts(account_id),
                    amount DECIMAL(15, 2) NOT NULL, 
                    currency_code varchar(3) REFERENCES Currencies(currency_code)
                );""")
        
            # Currencies Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS Currencies (
                    currency_code varchar(3),
                    currency_name varchar(50),
                    added_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);""")

            conn.commit()
            logger.info("Database tables created successfully")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error creating tables: {e}")
        raise
    finally:
        if conn:
            cur.close()
            release_conn(conn)


'''
Helper functions
'''
def validate_amount(amount):
    '''
    Ensures that the amount inputted is valid

    Args:

        amount: the currency amount entered

    
    Result:
        True if amount is valid.

        Invalid amount if not
    '''
    if amount > 0:
        return True
    else:
        return "Please input valid amount"
    

def format_currency(amount, currency_code):
    '''
    Format currency according to its code

    Args:
        amount: Amount of currency

        currency code: Shows the currency we're working we

    Result:
        A string format of the currency in its base format
    '''

    currency_symbols = {
        'USD': '$',
        'EUR': 'E',
        'GBP': 'P',
        'JPY': 'Y'
    }

    symbol = currency_symbols.get(currency_code, '')
    
    if currency_code == 'JPY':
        return f"{symbol}{int(amount)}"
    else:
        return f"{symbol}{amount:.2f}"


def secure_password(password):
    '''
    Hashes the password

    Args:
        password: the user inputted password

    Returns:
        A hash of the given password
    
    '''
    b_pass = f"{password}".encode()
    sha256 = hashlib.sha256()
    sha256.update(b_pass)
    pass_hash = sha256.hexdigest()
    return pass_hash

def verify_password(password_hash, password_attempt):
    '''
    Verifies that login password is correct

    Args:
        Password_hash: The stored password hash

        password_attempt: Hash of the user inputted password

    Returns a Boolean to show if password is correct or not
    '''
    if password_attempt == password_hash:
        return True
    else:
        return False
    
    # API functions
def fetch_exchange_rate():
    '''
    Fetches exchange rates from the API
    '''
    import requests
    from requests.structures import CaseInsensitiveDict

    API_KEY = os.getenv("api_key")

    url = "https://api.freecurrencyapi.com/v1/latest?"
    headers = CaseInsensitiveDict()
    headers["apikey"] = API_KEY


    resp = requests.get(url, headers=headers)

    if resp.status_code == 200:

        data = resp.json()

        return data
    else:
        return "No data found"
