import dateutil.relativedelta
import pandas as pd
import datetime
import logging
import psycopg2
import utils
import uuid
import time
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='banking_system.log'
)
logger = logging.getLogger('banking_users')

class User():
    def __init__ (self, user_id, username, password, email, fullname, creation_date):
        email_pattern = r'^\w{1,50}@\w{1,50}\.(com|org|net)'

        if not re.match(email_pattern, email):
            raise ValueError ("Email not valid")
        
        self.user_id = user_id
        self.username = username
        self.password = password        
        self.email = email
        self.fullname = fullname
        self.creation_date = creation_date

    
    def create_user(self, username, password, email, fullname):
        '''
        Creates a user profile

        Args:
            Username: Unique name for all bank users
            password: Hashed string of the user's password
            email: User's mail
            fullname: Users fullname
        
        Returns:
            String with username attached
        '''
        
        try:
            conn = utils.connect_to_db()
            # Fetches list of users so as to get a unique user_id for the new user
            with conn.cursor() as cur:
                cur.execute("SELECT email, username FROM Users")
                rows = cur.fetchall()
            
            # Checks is username exists
            username_exists = False
            email_exists = False

            if rows != []:
                for row in rows:
                    if username == row[1]:
                        username_exists = True
            
            if rows:
                for row in rows:
                    if email == row[0]:
                        email_exists = True
            
            # Adds admin role for all users
            is_admin = False

            failed_attempts = 0
            last_login = None
                    
            if not username_exists and not email_exists:
                user_data = (
                        username,
                        password,
                        email,
                        fullname,
                        is_admin, 
                        failed_attempts,
                        last_login
                        )
                
                # Update Users table with user's details
                with conn.cursor() as cur:
                    cur.execute("""INSERT INTO Users(
                                username, password, email, fullname, is_admin, failed_attempts, last_login)
                                Values(%s, %s, %s, %s, %s, %s, %s) RETURNING user_id;""", user_data)
                    
                    rows = cur.fetchall()
                    
                conn.commit()
                logger.info("Profile created successfully")

                return f"Profile with {username} created successfully"
            else:
                return "Duplicate username. Please change the username"

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error creating profile: {e}")
            raise e

        finally:
            cur.close()
            utils.release_conn(conn)



    def authenticate_user(self, username, password):
        '''
        Ensures that person trying to login is indeed the user

        Args:
            username: Username of the user
            password: Hashed value of the password

        Returns:
            String detailing if login was successful or not
        '''
        conn = None
        try:
            conn = utils.connect_to_db()
            with conn.cursor() as cur:
                cur.execute("SELECT username, password, failed_attempts FROM Users WHERE username = %s;",
                            (username,))
                rows = cur.fetchone()
                
        except Exception as e:
            logger.error(f"Couldn't fetch username and password: {e}")
            raise
        finally:
            cur.close()
            utils.release_conn(conn)

        password_hash = rows[1]
        failed_attempts = rows[2]

        if failed_attempts and failed_attempts == 3:
            logger.info(f"User {username} is locked out")
            return "Account is locked. Please contact support"
        
        if rows[0] == username:
            result = utils.verify_password(password_hash, password)
            if result:
                try:
                    conn = utils.connect_to_db()
                    with conn.cursor() as cur:
                        cur.execute("UPDATE Users SET failed_attempts = 0 WHERE username = %s", (username,))

                        conn.commit()
                        return "Login Successful"
                except Exception as e:
                    logger.error(f"Unable to update failed attempts: {e}")
                finally:
                    cur.close()
                    utils.release_conn(conn)
            else:
                failed_attempts += 1
                try:
                    conn = utils.connect_to_db()
                    with conn.cursor() as cur:
                        cur.execute("UPDATE Users SET failed_attempts = %s WHERE username = %s",(failed_attempts, username))
                
                        conn.commit()
                        return "Wrong password."
                except Exception as e:
                    if conn:
                        conn.rollback()
                    logger.error(f"Unable to update failed attempts: {e}")
                    raise
                finally:
                    cur.close()
                    utils.release_conn(conn)    
        else:
            return "Account not found"
        
         
    def get_user_details(self, user_id):
        '''
        Gets users details

        Args:
            user_id: Id of user whose details you want

        Returns:
            A tuple containing user details
        '''
        conn = None
        try:
            conn = utils.connect_to_db()
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM Users WHERE user_id = %s",
                            (user_id,))
                rows = cur.fetchone()
        except Exception as e:
            logger.error(f"Error reading from database: {e}")
            raise
        finally:
            cur.close()
            utils.release_conn(conn)

        if rows["0"] == user_id:
            return rows
        else:
            return "User not found"


def update_user_details(user_id, field, value):
    '''
    ADMIN ONLY FUNCTION

    Updates user details based on the given field and value

    Args:
        user_id: ID of user whose details should be updated
        field: The specific info to be updated
        value: The data to replace already existing data

    Returns:
        Success or failure statement
    '''
    conn = None

    try:
        conn = utils.connect_to_db()
        
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM Users WHERE user_id = %s", (user_id))
            rows = cur.fetchone()
    
            if rows:
                user_id = rows[0]

            if user_id:
                if field == "username":
                    try:
                        cur.execute("UPDATE Users SET username = %s WHERE user_id = %s", (value, user_id))
                        conn.commit()

                        logger.info(f"Updated {field} successfully")
                        return f"Username updated to {value} successfully"
                    except Exception as e:
                            if conn:
                                conn.rollback()
                            logger.error(f"Update failed: {e}")
                            raise
                    finally:
                        cur.close()
                        
               
                elif field == "email":
                    email_pattern = r'^\w{1,50}@\w{1,50}$\.(com|net|org)'

                    if re.match(email_pattern, value):
                        try:
                            cur.execute("UPDATE Users SET email = %s WHERE user_id = %s", (value, user_id))
                            conn.commit()

                            logger.info(f"Updated {value} successfully")
                            return f"Email updated to {value} successfully"

                        except Exception as e:
                            if conn:
                                conn.rollback()
                            logger.error(f"Error while updating: {e}")
                            raise
                        finally:
                            cur.close()
                            
                    else:
                        raise ValueError ("Email Invalid")
                
                elif field == "fullname":
                    try:
                        cur.execute("UPDATE Users SET fullname = %s WHERE user_id = %s", (value, user_id))
                        conn.commit()
                    
                        logger.info(f"Updated {value} successfully")
                    
                    except Exception as e:
                        if conn:
                            conn.rollback()
                        logger.error(f"Error while updating: {e}")
                        raise
                    finally:
                        cur.close()
                        
    except Exception as e:
        logger.error(f"Error updating field: {e}")
        raise
    finally:
        cur.close()
        utils.release_conn(conn)
        

class Account():
    def __init__ (self, user_id, currency_code, is_active, initial_balance=0):
        self.user_id = user_id
        self.currency_code = currency_code
        self.initial_balance = initial_balance
        self.is_active = is_active
        

    def create_account(self, user_id, account_id, currency_code, initial_balance):
        '''
        Creates a currency account for a user
        
        Args:
            user_id: ID of the user that wants to create an account
            currency_code: THe currency that is being stored by the account
            is_active: BOOLEAN showing if account is open or closed. DEFAULTS to True
            initial_balance: Amount deposited when account is opened

        Returns:
            Success message with account id attached or failure message

        '''
        conn = None
        try:
            conn = utils.connect_to_db()
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, account_id, currency_code FROM Accounts WHERE user_id = %s;", (user_id,))
                rows = cur.fetchall()
        except Exception as e:
            logger.error(f"Failed to get users: {e}")
            raise
        finally:
            cur.close()
            utils.release_conn(conn)

        if rows:
            is_active = True

        account_exists = False
        
        if rows:
            for row in rows:
                if user_id == row[0]:
                    if currency_code == row[2]:
                        account_exists = True
                        break
        
        if not account_exists:
            try:
                conn = utils.connect_to_db()
                account_data = (
                    user_id,
                    account_id,
                    currency_code,
                    initial_balance,
                    is_active
                    )
                
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO Accounts(" \
                                "user_id, account_id, currency_code, balance, is_active) " \
                                "Values(%s, %s, %s, %s, %s)", (account_data))
                
                    conn.commit()
                    logger.info("Account created successfully")
                    return f"Created {account_id} successfully"
            except Exception as e:
                logger.error(f"Error creating account: {e}")
                raise
            finally:
                cur.close()
                utils.release_conn(conn)
        else:
            return "Account already exists"
        

    def get_accounts(self, user_id):
        '''
        Gets all accounts owned by a user

        Args:
            user_id: ID  of user

        Returns:
            Tuple containing lists of all accounts and their details
        '''        
        conn = None
        try:
            conn = utils.connect_to_db()
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM Accounts WHERE user_id = %s", (user_id))
                rows = cur.fetchone()

                return rows
        except Exception as e:
            logger.error(f"Couldn't fetch accounts: {e}")
            raise
        finally:
            cur.close()
            utils.release_conn(conn)

    def close_account(self, account_id, user_id):
        '''
        Deactivates account. Prevents it from participating in transactions

        Args:
            account_id: ID of account to be deactivated
            user_id: ID of account owner
        Returns:
            Success message if complete. Error if not
        '''
        conn = None
        
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE Accounts SET is_active = False" \
                            " WHERE account_id = %s AND user_id = %s;", (account_id, user_id))
                
                conn.commit()
                logger.info("Account closed successfully")
            return "Closed account successfully"
        except Exception as e:
            logger.error(f"Error in closing account: {e}")
            raise
        finally:
            cur.close()
            utils.release_conn(conn)

def deposit(user_id, account_id, amount):
    
    amt = utils.validate_amount(amount)
    tx_type = "Deposit"
    created_on = datetime.datetime.now()
    tx_id = str(uuid.uuid4()) + str(int(time.time()) * 1000)
    conn = None

    try:
        conn = utils.connect_to_db()
        with conn.cursor() as cur:
            cur.execute("SELECT balance, currency_code, status FROM Accounts WHERE account_id = %s AND user_id = %s;", (account_id, user_id))
            rows = cur.fetchone()
            balance = rows[0]
            currency_code = rows[1]
            is_active = rows[2]
            
            if is_active:
                if account_id:
                    if amt == True:
                        new_balance = balance + amount
                        symbol = utils.format_currency(amount, currency_code)

                        cur.execute("UPDATE Accounts SET balance = %s WHERE account_id = %s;", (new_balance, account_id))
                        cur.execute("INSERT INTO Transactions (tx_time, tx_id, type, from_user_id, from_account_id, to_user_id, to_account_id, amount, currency_code)" \
                                    "Values(%s, %s, %s, %s, %s, %s, %s, %s, %s);",
                                    (created_on, tx_id, tx_type, None, None, user_id, account_id, amount, currency_code))
                    
                        conn.commit()
                        logger.info("Deposit successful")
                        return f"Deposit of {symbol} successful"
                    else:
                        return "Amount Not valid"
                else:
                    return "Account not found. Check account_id."
            else:
                return "Account closed. Reach out to support to reopen."
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Unable to complete deposit: {e}")
        raise 
    finally:
        cur.close()
        utils.release_conn(conn)


def withdraw(user_id, account_id, amount):
    
    amt = utils.validate_amount(amount)
    created_on = datetime.datetime.now()
    tx_type = "Withdraw"
    tx_id = str(uuid.uuid4()) + str(int(time.time()) * 1000)
    conn = None

    try:
        conn = utils.connect_to_db()
        with conn.cursor() as cur:
            cur.execute("SELECT balance, currency_code, status FROM Accounts WHERE account_id = %s AND user_id = %s;", (account_id, user_id))
            rows = cur.fetchone()

            balance = rows[0]
            currency_code = rows[1]
            is_active = rows[2]

        if is_active:  
            if balance:
                if amount < balance:
                    new_balance = balance - amount
                    symbol = utils.format_currency(amount, currency_code)

                    with conn.cursor() as cur:
                        cur.execute("UPDATE Accounts SET balance = %s WHERE account_id = %s", (new_balance, account_id))
                        cur.execute("INSERT INTO Transactions (tx_time, tx_id, type, from_user_id, from_account_id, to_user_id, to_account_id, amount, currency_code)" \
                                    "Values(%s, %s, %s, %s, %s, %s, %s, %s, %s);",
                                    (created_on, tx_id, tx_type, user_id, account_id, None, None, amount, currency_code))

                        conn.commit()
                        logger.info("Withdrawal successful")                
                    return f"Withdrawal of {symbol} successful"
                else:
                    return "Insufficient funds. Please deposit"
            else:
                return "Account doesn't exist."
        else:
            return "Account is closed. Please reach out to support."
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Unable to withdraw: {e}")

        raise e
    finally:
        conn.close()


def transfer(source_account_id, target_account_id, from_user_id, to_user_id, amount):
    '''
    Transfers amount from one account to another in the same currency

    Args:
        source_account_id: Sending account
        target_account_id: Recipient account
        from_user_id: User initiating transfer
        to_user_id: Recipient of transfer
        amount: Amount of currency being transferred

    Returns:
        Success or failure message
    '''
    amt = utils.validate_amount(amount)
    created_on = datetime.datetime.now()
    tx_type = "Transfer"
    tx_id = str(uuid.uuid4()) + str(int(time.time()) * 1000)
    conn = None
    
    try:
        # Get a connection
        conn = utils.connect_to_db()
        
        with conn.cursor() as cur:
            # Fetch source account details from Database
            cur.execute("""SELECT balance, currency_code, is_active FROM Accounts WHERE account_id = %s AND user_id = %s""", 
                        (source_account_id, from_user_id))
            from_rows = cur.fetchone()
            
            if from_rows:
                from_balance = from_rows[0]
                from_code = from_rows[1]
                from_is_active = from_rows[2]

            # Fetch recipient account from Database
            cur.execute("""SELECT balance, currency_code, is_active FROM Accounts WHERE account_id = %s AND user_id = %s""", 
                        (target_account_id, to_user_id))
            to_rows = cur.fetchone()
            

            if to_rows:
                to_balance = to_rows[0]
                to_code = to_rows[1]
                to_is_active = to_rows[2]
    except Exception as e:
        logger.error(f"Error fetching info from database: {e}")
        raise
    finally:
        cur.close()
        utils.release_conn(conn)

    if from_is_active:
        if to_is_active:
            if from_balance > amount:
                if to_code == from_code:
                    if amt:
                        from_new_balance = from_balance - amount
                        to_new_balance = to_balance + amount
                        code = from_code

                        symbol = utils.format_currency(amount, code)

                        try:
                            conn = utils.connect_to_db()
                            with conn.cursor() as cur:
                                # Debit account
                                cur.execute("UPDATE Accounts SET balance = %s WHERE account_id = %s;", (from_new_balance, source_account_id))

                                # Credit account
                                cur.execute("UPDATE Accounts SET balance = %s WHERE account_id = %s", (to_new_balance, target_account_id))

                                # Add transaction to db
                                cur.execute("INSERT INTO Transactions (tx_time, tx_id, type, from_user_id, from_account_id, to_user_id, to_account_id, amount, currency_code)" \
                                            "Values (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
                                            (created_on, tx_id, tx_type, from_user_id, source_account_id, target_account_id, amount, code))
                    
                                conn.commit()

                                logger.info("Transfer complete")
                                return f"Transfer of {symbol} successful"
                        except Exception as e:
                            if conn:
                                conn.rollback()
                            logger.error(f"Error completing transfer: {e}")
                            raise 
                        finally:
                            cur.close()
                            utils.release_conn(conn)
                    else:
                        return "You can't transfer between two different currencies. Try Currency Exchange instead"
                else:
                    return "Insufficient balance. Please deposit"
            else:
                return "Target account is closed"
        else:
            return "Your account is closed"


def get_transaction_history(account_id, startdate=None, enddate=None):
    '''
    Gets transaction history for select account

    Args:
        account_id: Currency account of user logged in

    Returns:
        List containing transactions over time
    
    '''
    conn = None
    try:
        conn = utils.connect_to_db()

        with conn.cursor() as cur:
            cur.execute("SELECT account_id FROM Accounts WHERE account_id = %s", (account_id,))
            accounts = cur.fetchone()

            logger.info("Fetched account_id from Database")
    except Exception as e:
        logger.error(f"Error fetching data from Database: {e}")
        raise
    finally:
        cur.close()
        utils.release_conn(conn)

    if accounts:
        try:
            conn = utils.connect_to_db()
            # Fetch account's transactions from database
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM Transactions WHERE from_account_id = %s OR to_account_id = %s", (account_id, account_id))
                rows = cur.fetchall()

                result = rows
                logger.info("Transactions successfully fetched")
                return result
        except Exception as e:
            logger.error(f"Error in fetching transactions: {e}")
            raise
        finally:
            cur.close()
            utils.release_conn(conn)


# Currency Operations start here
def get_exchange_rate(to_currency, from_currency):
    '''
    Gets the exchange rate of the base currency respect to the quote currency

    Args:
        to_currency: Currency you want to transfer to
        from_currency: Currency you're transferring from

    Returns:
        The ratio of from_currency to to_currency
    '''

    resp = utils.fetch_exchange_rate()
    
    if to_currency  == "USD":
        
            usd_rate = resp["data"][from_currency]

            rate = 1/usd_rate
            return rate
            
    elif from_currency == "USD":
        usd_rate = resp["data"][to_currency]

        rate = usd_rate
        return rate
        
    else:
        to_rate = resp["data"][to_currency]
        from_rate = resp["data"][from_currency]

        rate = (to_rate / from_rate)

        return rate


def convert_currency(amount, from_currency, to_currency):
    '''
    Converts from one currency to another. Does not store the value.

    Args:
        amount: amount to be converted
        from_currency: Currency to be converted from
        to_currency: Currency to be converted to
    
    Returns:
        Success message including amount received and symbol
    '''
    rate = get_exchange_rate(to_currency, from_currency)

    if amount > 0:
        amount_received = (amount * rate)
        symbol = utils.format_currency(amount_received, to_currency)

        return amount_received, f"You have received {symbol} in your account"

def get_supported_currencies():
    '''
    Fetches the currencies currently supported by the bank

    Returns:
        A list of all the supported currencies
    '''
    conn = None
    try:
        conn = utils.connect_to_db()

        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT currency_code FROM Currencies")
            rows = cur.fetchall()

            logger.info("Fetched list of currencies")
    except Exception as e:
        logger.error(f"Error fetching currency list: {e}")
        raise
    finally:
        cur.close()
        utils.release_conn(conn)

    if not rows == []:
            currencies = list(rows)

            conn.close()

            return currencies
    else:
        currencies = []
        return []

def add_currency_code(currency_code):
    '''
    ADMIN ONLY FUNCTION
    Adds new currencies to be supported

    Args:
        currency_code: currency code to be added to the Currency table

    
    Returns:
        Success or failure string
    '''
    conn = None
    resp = utils.fetch_exchange_rate()
    
    if resp:
        codes = resp["data"].keys()
    
    try:
        conn = utils.connect_to_db()
        with conn.cursor() as cur:
            cur.execute("SELECT currency_code FROM Currencies")
            existing_codes = cur.fetchall()

    except Exception as e:
        logger.error(f"Unable to fetch results: {e}")
        raise
    finally:
        cur.close()
        utils.release_conn(conn)

    if currency_code not in existing_codes:
        if currency_code in codes:
            try:
                conn = utils.connect_to_db()
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO Currencies (currency_code) Values(%s)", (currency_code,))
            
                conn.commit()
                logger.info("Added currency successfully")
                return f"Added {currency_code} successfully"
            except Exception as e:
                logger.error(f"Unable to add currency: {e}")
                if conn:
                    conn.rollback()
                raise
            finally:
                cur.close()
                utils.release_conn(conn)
        else:
            return "Not supported"
    else:
        return "Currency already added"
      

def currency_exchange(account_id_from, account_id_to, to_user_id, from_user_id, amount):
    '''
    Converts from one currency to another and transfers to the user_given account

    Args:
        account_id_from: Sender's account
        account_id_to: Recipient's account
        to_user_id: Recipient's ID
        from_user_id: Sender's ID
        amount: Amount exchanged
    
    Returns:
        Formatted string with amount received and symbol

    '''
    conn = utils.connect_to_db()

    created_on = datetime.datetime.now()
    tx_type = "Conversion"
    tx_id = str(uuid.uuid4()) + str(int(time.time()) * 1000)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT balance, currency_code, status FROM Accounts WHERE account_id = %s AND user_id = %s", (account_id_from, from_user_id))
            from_rows = cur.fetchone()

            cur.execute("SELECT balance, currency_code, status FROM Accounts WHERE user_id = %s AND account_id = %s", (to_user_id, account_id_to))
            to_rows = cur.fetchone()
    except Exception as e:
        logger.error(f"Error fetching details from Database: {e}")
        raise
    finally:
        cur.close()
        utils.release_conn(conn)

    to_currency = to_rows[1]
    from_currency = from_rows[1]

    from_balance = from_rows[0]
    to_balance = to_rows[0]

    from_is_active = from_rows[2]
    to_is_active = to_rows[2]

    if to_currency == from_currency:
        return "Currencies must be different to be converted. Try Transfer instead"
    else:
        result, message = convert_currency(amount, to_currency, from_currency)
        from_symbol = utils.format_currency(amount, from_currency)
        to_symbol = utils.format_currency(result, to_currency)

        from_new_balance = from_balance - amount
        to_new_balance = to_balance + result

    if from_is_active:
        if to_is_active:
            try:
                conn = utils.connect_to_db()
                with conn.cursor() as cur:
                    cur.execute("UPDATE Accounts SET balance = %s WHERE account_id = %s", (from_new_balance, account_id_from))

                    cur.execute("UPDATE Accounts SET balance = %s WHERE account_id = %s AND user_id = %s", 
                                (to_new_balance, account_id_to, to_user_id))

                    cur.execute("INSERT INTO Transactions (tx_time, tx_id, type, from_user_id, from_account_id, to_user_id, to_account_id, amount, currency_code)" \
                                "Values(%s, %s, %s, %s, %s, %s, %s, %s, %s)", (created_on, tx_id, tx_type, from_user_id, account_id_from, None, None, amount, from_currency))
        
                    cur.execute("INSERT INTO Transactions (tx_time, tx_id, type, from_user_id, from_account_id, to_user_id, to_account_id, amount, currency_code)" \
                                "Values(%s, %s, %s, %s, %s, %s, %s, %s, %s)", (created_on, tx_id, tx_type, None, None, to_user_id, account_id_to, amount, to_currency))
        
                    conn.commit()

                    logger.info("Exchange successful")
                    return f"Successfully exchanged {from_symbol} to {to_symbol} complete"
            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"Exchange failed: {e}")
                raise 
            finally:
                cur.close()
                utils.release_conn(conn)
        else:
            return "Target account is closed"
    else:
        return "YOur account is closed"


# Analytics and Reporting 
def get_account_balance_history(account_id, user_id, period):
    '''
    

    Args:
        account_id: Account id for the selected currency
        user_id: Owner of selected account
        period: Timeframe for analysis(Monthly, Weekly, Daily)
    
    Returns:
        A dictionary of date and balance
    '''
    conn = None

    # Fetch transactions for account
    try:
        conn = utils.connect_to_db()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM Transactions WHERE (to_account_id = %s OR from_account_id = %s)" \
                        " AND (from_user_id = %s OR to_user_id = %s)", (account_id, account_id, user_id, user_id))
            rows = cur.fetchone()

            # Fetch current day balance
            cur.execute("SELECT balance FROM Accounts WHERE account_id = %s AND user_id = %s ;", (account_id, user_id))
            balances = cur.fetchone()

            logger.info("Returned balances")
            balance = balances[0]
    except Exception as e:
        logger.error(f"Failed to fetch balances: {e}")
        raise
    finally:
        cur.close()
        utils.release_conn(conn)

    if not rows:
        return "No transactions recorded"
    
    if rows:
        current_day = datetime.date.today()
        result = {}
        result[current_day] = balance
        conn = utils.connect_to_db()

        # Loop for monthly analysis
        if period == 'monthly':
            for _ in range(5):
                start_day = current_day - dateutil.relativedelta.relativedelta(months=1)
                stop_day = current_day

                # Gets net flows over given monthly
                try:
                    
                    with conn.cursor() as cur:
                        cur.execute("""WITH deposits AS (
                                    SELECT COALESCE(SUM(amount),0) AS amount_dep FROM Transactions 
                                    WHERE to_account_id = %s AND to_user_id = %s 
                                    AND (tx_time::date) >= %s AND (tx_time::date) <= %s),
                                
                                    withdrawals AS ( 
                                    SELECT COALESCE(SUM(amount),0) as amt_with FROM Transactions 
                                    WHERE from_account_id = %s AND from_user_id = %s 
                                    AND (tx_time::date) >= %s AND (tx_time::date) <= %s) 
                            
                                    SELECT (d.amount_dep - w.amt_with) FROM deposits AS d \
                                    CROSS JOIN withdrawals AS w;""",
                                    (account_id, user_id, start_day, stop_day, account_id, user_id, start_day, stop_day))
                    
                    rows = cur.fetchone()
                    net_tx = rows[0] if rows[0] is not None else 0   

                    hist_balance = balance - net_tx                 
                    result[start_day] = hist_balance

                    current_day = start_day
                    logger.info("Fetched details successfully")

                except Exception as e:
                    logger.error(f"Failed to fetch details: {e}")
                    raise
                finally:
                    cur.close()
                    utils.release_conn(conn)

        sorted_result = dict(sorted(result.items()))
        return sorted_result

    # Loop for weekly timeframe
    elif period == 'weekly':
        for _ in range(5):
            
            start_day = current_day - dateutil.relativedelta.relativedelta(weeks=1)
            stop_day = current_day
            
            # Fetch net flows over weekly timeframe
            try:
                
                with conn.cursor() as cur:
                    cur.execute("""WITH deposits AS (
                                SELECT COALESCE(SUM(amount),0) AS amount_dep FROM Transactions 
                                WHERE to_account_id = %s AND to_user_id = %s 
                                AND (tx_time::date) >= %s AND (tx_time::date) <= %s),
                                    
                                withdrawals AS ( 
                                SELECT COALESCE(SUM(amount),0) as amt_with FROM Transactions 
                                WHERE from_account_id = %s AND from_user_id = %s 
                                AND (tx_time::date) >= %s AND (tx_time::date) <= %s) 
                            
                                SELECT (d.amount_dep - w.amt_with) FROM deposits AS d \
                                CROSS JOIN withdrawals AS w;""",
                            (account_id, user_id, start_day, stop_day, account_id, user_id, start_day, stop_day))
                    
                rows = cur.fetchone()
                net_tx = rows[0] if rows[0] is not None else 0   
                hist_balance = balance - net_tx                 

                result[start_day] = hist_balance
                current_day = start_day

                logger.info("Fetched details successfully")

            except Exception as e:
                logger.error(f"Failed to fetch details: {e}")
                raise
            finally:
                cur.close()
                utils.release_conn(conn)

        sorted_result = dict(sorted(result.items()))
        return sorted_result
    
    # Loop for daily analysis
    else:
        for _ in range(5):
            start_day = current_day - dateutil.relativedelta.relativedelta(days=1)
            stop_day = current_day
            
            # Fetch net flows over given timeframe
            try:
                with conn.cursor() as cur:
                    cur.execute("""WITH deposits AS (
                                SELECT COALESCE(SUM(amount),0) AS amount_dep FROM Transactions 
                                WHERE to_account_id = %s AND to_user_id = %s 
                                AND (tx_time::date) >= %s AND (tx_time::date) <= %s),
                                
                                withdrawals AS ( 
                                SELECT COALESCE(SUM(amount),0) as amt_with FROM Transactions 
                                WHERE from_account_id = %s AND from_user_id = %s 
                                AND (tx_time::date) >= %s AND (tx_time::date) <= %s) 
                                
                                SELECT (d.amount_dep - w.amt_with) FROM deposits AS d \
                                CROSS JOIN withdrawals AS w;""",
                                (account_id, user_id, start_day, stop_day, account_id, user_id, start_day, stop_day))
                    
                    rows = cur.fetchone()
                    net_tx = rows[0] if rows[0] is not None else 0   

                    hist_balance = balance - net_tx                 

                    result[start_day] = hist_balance

                    current_day = start_day
                    logger.info("Fetched details successfully")

            except Exception as e:
                logger.error(f"Failed to fetch details: {e}")
                raise
            finally:
                cur.close()
                utils.release_conn(conn)
        sorted_result = dict(sorted(result.items()))
        return sorted_result

def get_spending_history(account_id, user_id, start_date, end_date):
    '''
    Gets the spending history of selected account

    Args:
        account_id: Account being checked
        user_id: Owner of the account
        start_date: Date to begin calculations on
        end_date: Date to end calculations on
    
    Returns:
        A list of dictionaries containing Date and amount
    '''
    
    conn = utils.connect_to_db()

    result = []
    has_transactions = False
    
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM Transactions WHERE from_account_id = %s AND from_user_id = %s " \
                        "AND (tx_time::date) >= %s AND (tx_time::date) <= %s", (account_id, user_id, start_date, end_date))
            rows = cur.fetchall()

            if rows:
                has_transactions = True
        if has_transactions:
            with conn.cursor() as cur:
                cur.execute("SELECT (tx_time::date) as Day, COALESCE(SUM(amount),0) FROM Transactions " \
                            "WHERE from_account_id = %s AND from_user_id = %s " \
                            "AND (tx_time::date) >= %s AND (tx_time::date) <= %s " \
                            "GROUP BY 1 ORDER BY 1 ASC", (account_id, user_id, start_date, end_date))
                rows = cur.fetchall()

            if rows:
                for row in rows:
                    result.append({
                        "Date": row[0],
                        "Amount": row[1]
                    })                
        else:
            return "No transactions yet"

    except Exception as e:
        raise e
    finally:
        conn.close()

    return result

def generate_account_statement(account_id, user_id, start_date, end_date):
    '''
    Displays all transactions occurring within given time

    Args:
        account_id: ID of account being checked
        user_id: ID of account owner
        start_date: Date to begin calculations on
        end_date: Date to stop calculations on
    
    Returns:   
        A list of dictionaries containing transactions within specified timeframe
    '''
    conn = None

    try:
        conn = utils.connect_to_db()
        with conn.cursor() as cur:
            cur.execute("WITH net_tx AS (" \
                        "SELECT SUM(amount) AS amount FROM Transactions " \
                        "WHERE to_account_id = %s AND (tx_time::date) <= %s " \
                        "UNION ALL " \
                        "SELECT SUM(-1 * (amount)) AS amount FROM Transactions " \
                        "WHERE from_account_id = %s AND (tx_time::date) <= %s)" \
                        " " \
                        "SELECT SUM(amount) FROM net_tx", (account_id, start_date, account_id, start_date))
        
        rows = cur.fetchone()
        has_transactions = False
    except Exception as e:
        logger.error(f"Failed to check transactions: {e}")
        raise
    finally:
        cur.close
        

    if rows:
        has_transactions = True
        opening_balance = rows[0]  

        result = []
        result.append({
            "Date": start_date, 
            "Type": None, 
            "From": None, 
            "Account": None, 
            "To": None, 
            "Account": None, 
            "Amount": None, 
            "Opening Balance": opening_balance})

    if has_transactions:
        try:
            with conn.cursor() as cur:
                cur.execute("""WITH total_tx AS (
                            SELECT tx_time, type, from_user_id, from_account_id, to_user_id, to_account_id, COALESCE(SUM(amount),0) AS amount 
                            FROM Transactions 
                            WHERE to_account_id = %s AND to_user_id = %s 
                            AND (tx_time::date) >= %s AND (tx_time::date) <=%s
                            GROUP BY 1,2,3,4,5,6 
                            UNION ALL 
                            SELECT tx_time, type, from_user_id, from_account_id, to_user_id, to_account_id, COALESCE(SUM(-1 *(amount)), 0) AS amount 
                            FROM Transactions 
                            WHERE from_account_id = %s AND from_user_id = %s 
                            AND (tx_time::date) >= %s AND (tx_time::date) <= %s
                            GROUP BY 1,2,3,4,5,6) 
                                
                            SELECT tx_time, type, from_user_id, from_account_id, to_user_id, to_account_id, SUM(amount) as amount 
                            FROM total_tx 
                            GROUP BY 1,2,3,4,5,6 ORDER BY 1 ASC""", 
                            (account_id, user_id, start_date, end_date, account_id, user_id, start_date, end_date))
                    
                tx_rows = cur.fetchall()
                
            if tx_rows:
                for row in tx_rows:                    
                    result.append({
                        "Date": row[0],
                        "Type": row[1],
                        "From": row[2],
                        "From Account": row[3],
                        "To": row[4],
                        "To Account": row[5],
                        "Amount": (-1*row[6]) if row[4] is None else row[6],
                        "Balance": ((opening_balance + row[6]) if opening_balance is not None else (0 + row[6]))
                    })
                logger.info("Result successfully gotten")
                return result
        except Exception as e:
            logger.error(f"Failed to fetch results: {e}")
            raise
        finally:
            cur.close
            utils.release_conn(conn)
    
    else:
        return "No transactions yet"
    

# Godspeed