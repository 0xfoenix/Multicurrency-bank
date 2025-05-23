from datetime import datetime
import pandas as pd
import logging
import getpass
import users
import utils
import sys

'''
Logging setup
'''
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=('banking_system.log')
)
logger = logging.getLogger("banking_cli")

# For storing logged in user
current_user = None

def Bank_App():
    global current_user

    while True:
        if not current_user:
            print("\n--Welcome to the Royal Bank--")
            print("1. Create Profile")
            print("2. Login")
            print("3. Close")

            choice = input("Select an option: ")

            match choice:
                # Create Account menu
                case "1":
                    username = input("Enter your username: ")
                    input_password = getpass.getpass("Enter your password: ")
                    email = input("Enter your email: ")
                    first_name = input('Enter your first name: ')
                    last_name = input("Enter your last name: ")

                    if len(input_password) >= 8:
                        password = utils.secure_password(input_password)
                    else:
                        print("Password must have at least 8 digits")

                    fullname = first_name + ' ' + last_name

                    if username and input_password and email and fullname:
                        user = users.User(1, username, 1, email, fullname, datetime.now())
                        result = user.create_user(username, password, email, fullname)
            
                        logger.info(f"{result}")
                        print(result)
                        return result
                    else:
                        print("Please fill in the necessary details")
            
                case "2":
                    # Login menu
                    username = input("Enter your username: ")
                    input_password = getpass.getpass("Enter your password: ")

                    if username and input_password:
                        if len(input_password) >= 8:
                            password = utils.secure_password(input_password)
                            user = users.User(1, username, 1, 'email@emai.com', "email", datetime.now())

                            result = user.authenticate_user(username, password)
                            print(result)

                            if result == "Login Successful":
                                try:
                                    conn = utils.connect_to_db()
                                    with conn.cursor() as cur:
                                        cur.execute("SELECT user_id FROM Users WHERE username = %s", (username,))
                                        rows = cur.fetchone()
                                        user_id = rows[0]

                                except Exception as e:
                                    logger.error(f"Failed to get user_id: {e}")
                                    raise
                                finally:
                                    utils.release_conn(conn)

                                current_user = user_id
                                logger.info(f"{current_user} logged in")
                                
                            else:
                                print(result)                           
                        else:
                            print("Password must be 8 characters")
                    else:
                        print("Input all the necessary details")
                case "3":
                    # Exit the program
                    sys.exit(0)

        if current_user:
            try:
                conn = utils.connect_to_db()
                with conn.cursor() as cur:
                    cur.execute("SELECT username FROM Users WHERE user_id = %s", (current_user,))
                    rows = cur.fetchone()

                if rows:
                    username = rows[0]
            except Exception as e:
                logger.error(f"Unable to fetch username from DB: {e}")
                raise
            finally:
                utils.release_conn(conn)

            print(f"Welcome to the Royal Bank, {username}")
            print("1. Create Account")
            print("2. Deposit")
            print("3. Withdraw")
            print("4. Transfer")
            print("5. View Balance")
            print("6. Update User Details")
            print("7. Get Exchange Rate")
            print("8. Convert Currency")
            print("9. Currency Exchange")
            print("10. Add New Currency")
            print("11. Account Analytics")
            print("12. Close Account")
            print("13. Logout")
            print("14. Quit")
            
            main_choice = input("Select an option: ")

            match main_choice:
                # Create Account menu
                case "1":
                    try:
                        #Connect to DB
                        conn = utils.connect_to_db()

                        # Get next account_id
                        with conn.cursor() as cur:
                            cur.execute("SELECT account_id, currency_code FROM Accounts WHERE user_id = %s;", (current_user,))
                            account_rows = cur.fetchall()

                            account_id = len(account_rows) + 1
                
                            cur.execute("SELECT currency_code FROM Currencies")
                            rows = cur.fetchall()
                            codes = list(row[0].strip(',') for row in rows)

                            for i, code in enumerate(codes):
                                print(f"{i}. {code}")
                            
                            select_currency = input("Select a currency: ")
                            
                            # Checks if response is digit and gets the corresponding currency code
                            if select_currency.isdigit():
                                index = int(select_currency)
                                
                                if 0 <= index < len(codes):
                                    currency = codes[index]
                                    
                            else:
                                if select_currency in codes:
                                    # Converts currency code to uppercase
                                    currency = select_currency.upper()
                                else:
                                    print("Invalid currency")

                            print(currency)

                            initial_deposit = int(input("Select an amount to deposit: "))
                            
                            # Checks if initial deposit is int not float
                            is_int = False
                            if isinstance(initial_deposit, int):
                               is_int = True 
                            else:
                                print("Not a valid number. Please input a whole number")

                            # Main create user logic
                            if current_user:
                                if currency and is_int:
                                    is_active = True

                                    account = users.Account(current_user, currency, is_active, initial_deposit)

                                    result = account.create_account(current_user, account_id, currency, 0)

                                    # Get account_id if was created successfully
                                    try: 
                                        with conn.cursor() as cur:
                                            cur.execute("SELECT account_id FROM Accounts WHERE user_id = %s AND currency_code = %s",
                                                        (current_user, currency))

                                            rows = cur.fetchone()

                                        
                                        # Checks if account exists then deposit
                                        if rows:
                                            if result != "Account already exists":
                                                account_id = rows[0]
                                                result = users.deposit(current_user, account_id, initial_deposit)

                                                print(f"{result}")
                                            else:
                                                print("Account already exists")
                                    except Exception as e:
                                        logger.error(f"Failed to deposit to account: {e}")
                                        raise
                                    finally:
                                        utils.release_conn(conn)
                                else:
                                    print("Please input the necessary details")
                            else:
                                print("Please Login")
                    except Exception as e:
                        logger.error(f"Failed to create account: {e}")
                        raise
               
                # Deposit Menu
                case "2":
                    try:
                        # Connect to DB
                        conn = utils.connect_to_db()
                        with conn.cursor() as cur:
                            cur.execute("SELECT DISTINCT currency_code FROM Accounts WHERE user_id = %s AND is_active = TRUE", 
                                        (current_user,))
                            rows = cur.fetchall()

                        currencies = list(row[0].strip(',') for row in rows)
                        
                        for i, code in enumerate(currencies):
                            print(f"{i}. {code}")

                        select_currency = input("Select an option: ")
                        amount = int(input("Enter amount to deposit: "))

                        # Checks if response is digit and gets the corresponding currency code
                        if select_currency.isdigit():
                            index = int(select_currency)
                                
                            if 0 <= index < len(currencies):
                                currency = currencies[index]              
                        else:
                            if select_currency in currencies:
                                # Converts currency code to uppercase
                                currency = select_currency.upper()

                        with conn.cursor() as cur:
                            cur.execute("SELECT account_id WHERE user_id = %s AND currency_code = %s", (current_user, currency))
                            rows = cur.fetchone()

                            if rows:
                                account_id = rows[0]

                                result = users.deposit(current_user, account_id, amount)
                                print(result)
                                logger.info("Fetched account details successfully")
                            else:
                                print("Account doesn't exist")
                    except Exception as e:
                        logger.error(f"Failed to deposit: {e}")
                        raise                        
                    finally:
                        utils.release_conn(conn)
               
                # Withdraw Menu
                case "3":
                    try:
                        # Connect to DB
                        conn = utils.connect_to_db()
                        with conn.cursor() as cur:
                            cur.execute("SELECT DISTINCT currency_code FROM Accounts WHERE user_id = %s AND is_active = TRUE", 
                                        (current_user,))
                            rows = cur.fetchall()

                        currencies = list(row[0].strip(',') for row in rows)
                        
                        for i, code in enumerate(currencies):
                            print(f"{i}. {code}")

                        select_currency = input("Select an option: ")
                        amount = int(input("Enter amount to withdraw: "))

                        # Checks if response is digit and gets the corresponding currency code
                        if select_currency.isdigit():
                            index = int(select_currency)
                                
                            if 0 <= index < len(currencies):
                                currency = currencies[index]              
                        else:
                            if select_currency in currencies:
                                # Converts currency code to uppercase
                                currency = select_currency.upper()
                            else:
                                print("Invalid selection. Check your selection")

                        with conn.cursor() as cur:
                            cur.execute("SELECT account_id WHERE user_id = %s AND currency_code = %s", (current_user, currency))
                            rows = cur.fetchone()

                            if rows:
                                account_id = rows[0]

                                result = users.withdraw(current_user, account_id, amount)
                                print(result)
                                logger.info("Fetched account details successfully")
                            else:
                                print("Account doesn't exist")
                    except Exception as e:
                        logger.error(f"Failed to withdraw: {e}")
                        raise                        
                    finally:
                        utils.release_conn(conn)
               
                # Transfer Menu
                case "4":
                    try:
                        conn = utils.connect_to_db()

                        with conn.cursor() as cur:
                            cur.execute("SELECT DISTINCT currency_code FROM Accounts WHERE user_id = %s AND is_active = TRUE", 
                                        (current_user,))
                            rows = cur.fetchall()

                            if rows:
                                codes = list(currency[0].strip(',') for currency in rows)

                            for i, code in enumerate(codes):
                                print(f"{i}. {code}")
                            
                            select_currency = input("Select a currency: ")
                            
                            # Checks if response is digit and gets the corresponding currency code
                            if select_currency.isdigit():
                                index = int(select_currency)
                                
                                if 0 <= index < len(codes):
                                    currency = codes[index]
                                    
                            else:
                                if currency in codes:
                                    # Converts currency code to uppercase
                                    currency = select_currency.upper()
                                else:
                                    print("Invalid selection. Check yor selection again")

            
                            username = input("Enter target username: ")
                            amount = int(input("Enter amount: "))
            
                        if current_user:
                            if amount > 0:

                                try:
                                    # Fetch from_account_id for currency
                                    with conn.cursor() as cur:
                                        cur.execute("SELECT account_id FROM Accounts WHERE user_id = %s AND currency_code = %s;", 
                                                    (current_user, currency))
                                        rows = cur.fetchone()
                                    if rows:
                                        from_account_id = rows[0]                                    

                                        # Fetch to_user_id
                                        cur.execute("SELECT user_id FROM Users WHERE username = %s", (username,))
                                        rows = cur.fetchone()
                                    if rows:
                                        to_user_id = rows[0]
                                
                                        # Fetch to_account_id
                                        cur.execute("SELECT account_id FROM Accounts WHERE user_id = %s AND currency_code = %s;", 
                                                    (to_user_id, currency))
                                        rows = cur.fetchone()
                                    if rows:
                                        to_account_id = rows[0]
                                    else:
                                        print("No account exists for that currency")

                                except Exception as e:
                                    logger.error(f"Failed to fetch details: {e}")
                                    raise
                                finally:
                                    cur.close()
                                    utils.release_conn(conn)
                            
                                if currency and to_account_id and amount:
                                    result = users.transfer(from_account_id, to_account_id, amount)

                                    print(result)
                                else:
                                    print("Fill in the necessary details")
                            else:
                                print("Enter an amount greater than zero")
                        else:
                            print("Please login")
                    except Exception as e:
                        logger.error(f"Unable to withdraw: {e}")
                        raise
               
                # View Balance
                case "5":
                    try:
                        conn = utils.connect_to_db()
                        with conn.cursor() as cur:
                            cur.execute("SELECT DISTINCT currency_code FROM Accounts WHERE user_id = %s;", (current_user,))
                            rows = cur.fetchall()

                            codes = list(row[0].strip(',') for row in rows)

                            for i, code in enumerate(codes):
                                print(f"{i}. {code}")
                            
                            select_currency = input("Select a currency: ")

                            # Checks if response is digit and gets the corresponding currency code
                            if select_currency.isdigit():
                                index = int(select_currency)
                                
                                if 0 <= index < len(codes):
                                    currency = codes[index]
                                    
                            else:
                                if currency in codes:
                                    # Converts currency code to uppercase
                                    currency = select_currency.upper()
                                else:
                                    print("Unknown currency. Check your selection")

                    except Exception as e:
                            logger.error(f"Error selecting currencies: {e}")
                            raise
                    finally:
                        utils.release_conn(conn)

                    if current_user:
                        if currency:
                            # Fetch balance of currency from database
                            try: 
                                conn = utils.connect_to_db()
                                with conn.cursor() as cur:
                                    print(currency)
                                    cur.execute("SELECT balance FROM Accounts WHERE user_id = %s AND currency_code = %s;",
                                                (current_user, currency))
                                    rows = cur.fetchone()

                                balance = rows[0]
                                symbol = utils.format_currency(balance, currency)  

                                logger.info("Fetched balance successfully")
                                print(f"You have {symbol} remaining")
                            except Exception as e:
                                logger.error(f"Failed to fetch details: {e}")
                                raise
                            finally:
                                cur.close()
                                utils.release_conn(conn)
                        else:
                            print("Please select an account")
                    else:
                        print("Please login")
               
                # Update User Details
                case "6":
                    try:
                        conn = utils.connect_to_db()

                        # Checks if user is admin
                        with conn.cursor() as cur:
                            cur.execute("SELECT is_admin FROM Users WHERE user_id = %s", (current_user,))
                            rows = cur.fetchone()

                            is_admin = rows[0]
                    except Exception as e:
                        logger.error(f"Unable to fetch admin status: {e}")

                        if not is_admin:
                            print("Please reach out to customer support for this or visit our nearest office")
                        
                        else:
                            username = input("Enter username: ")
                            # Get user_id for input username
                            try:
                                with conn.cursor() as cur:
                                    cur.execute("SELECT user_id FROM Users WHERE username = %s", (username, ))
                                    rows = cur.fetchone()

                                if rows:
                                    user_id = rows[0]

                            except Exception as e:
                                logger.error(f"Unable to fetch user_id: {e}")        
                                raise
                            finally:
                                utils.release_conn(conn)
                            

                        all_fields = ["username", "email", "fullname"]
                            
                        for i, fields in all_fields:
                            print(f"{i}. {fields}")
                                
                        input_field = input("Select field to update")

                        if input_field.isdigit():
                            index = int(input_field)

                            if 0 <= index < len(all_fields):
                                field = all_fields[index]
                        else:
                            if input_field in all_fields:
                                field = input_field.lower()
                            else:
                                print("Please check your selected field")

                        value = input("Enter the new details: ")

                        
                        if current_user:
                            if username and field and value:
                                result = users.update_user_details(user_id, field, value)
                                print(result)

                            else:
                                print("Input the necessary details")
                        else:
                            print("Please login")
               
                # Get Exchange Rate
                case "7":
                    all_currencies = users.get_supported_currencies()
                    currencies = list(code[0].strip(',') for code in all_currencies)

                    for i, currency in enumerate(currencies):
                        print(f"{i}. {currency}")
                    
                    input_from_currency = input("Select from_currency: ")
                    
                    # Checks if from_currency is digit and returns corresponding code
                    if input_from_currency.isdigit():
                        index = int(input_from_currency)
                                
                        if 0 <= index < len(currencies):
                            from_currency = currencies[index]
                                    
                    else:
                        # If from_currency is in currencies, then return from_currency
                        if input_from_currency in currencies:
                            # Converts currency code to uppercase
                            from_currency = input_from_currency.upper()
                        else:
                            print("Unknown currency. Check your selection")
                    
                    input_to_currency = input("Select to_currency: ")
                    
                    # Checks if to_currency is digit and returns corresponding code
                    if input_to_currency.isdigit():
                        index = int(input_to_currency)
                                
                        if 0 <= index < len(currencies):
                            to_currency = currencies[index]
                                    
                    else:
                        # If from_currency is in currencies, then return from_currency
                        if input_to_currency in currencies:
                            # Converts currency code to uppercase
                            to_currency = input_to_currency.upper()
                        else:
                            print("Unknown currency. Check your selection")

                    
                    if current_user:
                        if from_currency and to_currency:   
                            result = users.get_exchange_rate(to_currency, from_currency)

                            print(result)
                        else:
                            print("Input the necessary details")
                    else:
                        print("Please login")

                # Convert Currency
                case "8":
                    print("""This is just a simulation to show you how much you gain when you exchange. 
                            Use the Currency exchange instead for real value""")
                    
                    all_currencies = users.get_supported_currencies()
                    currencies = list(code[0].strip(',') for code in all_currencies)
                    for i, currency in enumerate(currencies):
                        print(f"{i}. {currency}")
                    
                    input_from_currency = input("Select from_currency: ")
                    
                    # Checks if from_currency is digit and returns corresponding code
                    if input_from_currency.isdigit():
                        index = int(input_from_currency)
                                
                        if 0 <= index < len(currencies):
                            from_currency = currencies[index]
                                    
                    else:
                        # If from_currency is in currencies, then return from_currency
                        if input_from_currency in currencies:
                            # Converts currency code to uppercase
                            from_currency = input_from_currency.upper()
                        else:
                            print("Unknown currency. Check your selection")
                    
                    input_to_currency = input("Select to_currency: ")
                    
                    # Checks if to_currency is digit and returns corresponding code
                    if input_to_currency.isdigit():
                        index = int(input_to_currency)
                                
                        if 0 <= index < len(currencies):
                            to_currency = currencies[index]
                                    
                    else:
                        # If from_currency is in currencies, then return from_currency
                        if input_to_currency in currencies:
                            # Converts currency code to uppercase
                            to_currency = input_to_currency.upper()
                        else:
                            print("Unknown currency. Check your selection")

                    input_amount = int(input("Enter amount to convert: "))

                    if isinstance(input_amount, int):
                        amount = input_amount
                    
                    if current_user:
                        if from_currency and to_currency and amount:   
                            result = users.convert_currency(amount, from_currency, to_currency)

                            print(result)
                        else:
                            print("Input the necessary details")
                    else:
                        print("Please login")

                # Currency Exchange
                case "9":
                    
                    
                    try:
                        print("Leave blank if you want to transfer to yourself or enter your username.")
                    
                        username = input("Enter target user: ")
                        conn = utils.connect_to_db()
                        if username:
                            # Fetches the user_id from the DB
                            with conn.cursor() as cur:
                                cur.execute("SELECT user_id FROM users WHERE username = %s", (username, ))
                                rows = cur.fetchone()

                                if rows:
                                    to_user_id = rows[0]
                                else:
                                    print("User doesn't exist")
                                    break

                        else:
                            to_user_id = current_user                        

                        # Fetches user's active currencies from DB
                        with conn.cursor() as cur:
                            cur.execute("SELECT DISTINCT currency_code FROM Accounts WHERE user_id = %s AND is_active = TRUE", 
                                        (current_user,))
                            rows = cur.fetchall()

                        if rows:
                            currencies = list(row[0].strip(',') for row in rows)
                            
                        for i, code in enumerate(currencies):
                            print(f"{i}. {code}")

                        input_from_currency = input("Select currency to exchange: ")

                         # Checks if from_currency is digit and returns corresponding code
                        if input_from_currency.isdigit():
                            index = int(input_from_currency)
                                
                            if 0 <= index < len(currencies):
                                from_currency = currencies[index]

                        else:
                            # If from_currency is in currencies, then return from_currency
                            if input_from_currency in currencies:
                                # Converts currency code to uppercase
                                from_currency = input_from_currency.upper()
                            else:
                                print("Unknown currency. Check your selection")
                            
                    
                        input_to_currency = input("Select to_currency: ")

                        # Checks if to_currency is digit and returns corresponding code
                        if input_to_currency.isdigit():
                            index = int(input_to_currency)
                                
                            if 0 <= index < len(currencies):
                                to_currency = currencies[index]
                                    
                        else:
                            # If to_currency is in currencies, then return to_currency
                            if input_to_currency in currencies:
                                # Converts currency code to uppercase
                                to_currency = input_to_currency.upper()
                            else:
                                print("Unknown currency. Check your selection")

                        # Fetches account_id for sender
                        with conn.cursor() as cur:
                            cur.execute("SELECT account_id FROM Accounts WHERE user_id = %s AND currency_code = %s", 
                                        (current_user, from_currency))
                            rows = cur.fetchone()

                            if rows:
                                from_account_id = rows[0]

                        # Fetches account_id for receiver
                        with conn.cursor() as cur:
                            cur.execute("SELECT account_id FROM Accounts WHERE user_id = %s AND currency_code = %s",
                                        (to_user_id, to_currency))
                            rows = cur.fetchone()

                            if rows:
                                to_account_id = rows[0]
                            else:
                                print("Account doesn't exist")
                                break

                        input_amount = int(input("Enter amount to convert: "))

                        if isinstance(input_amount, int):
                            amount = input_amount
                    
                        if current_user:
                            if from_currency and to_currency and amount:   
                                result = users.currency_exchange(from_account_id, to_account_id, to_user_id, current_user, amount)

                                logger.info(f"{result}")
                                print(result)
                            else:
                                print("Input the necessary details")
                        else:
                            print("Please login")
                    except Exception as e:
                        logger.error(f"Error completing exchange: {e}")
                        raise
                    finally:
                        utils.release_conn(conn)

                # Add New Currency
                case "10":
                    try:
                        conn = utils.connect_to_db()

                        # Fetches admin status from DB
                        with conn.cursor() as cur:
                            cur.execute("SELECT is_admin FROM Users WHERE user_id = %s", (current_user,))
                            rows = cur.fetchone()

                        if rows:
                            is_admin = rows[0]
                        
                        if is_admin:
                            new_currency = input("Enter new currency")

                            result = users.add_currency_code(new_currency)

                            print(result)
                        else:
                            print("You cannot access this menu")
                    except Exception as e:
                        logger.error(f"Unable to add currency: {e}")
                        raise
                    finally:
                        utils.release_conn(conn)

                # Account Analytics
                case "11":
                    acc_list = ["Spending History", "Account Statement"]
                    
                    for i, acc in enumerate(acc_list):
                        print(f"{i}. {acc}")
                    
                    input_analysis = input("Enter your selection: ")
                    
                    # Checks if input_analysis is digit and returns corresponding string
                    if input_analysis.isdigit():
                        index = int(input_analysis)
                                
                        if 0 <= index < len(acc_list):
                            analysis = acc_list[index]
                                    
                    else:
                        # If input_analysis is in acc_list, then return analysis
                        if input_analysis in acc_list:
                            # Converts analysis to title case
                            analysis = input_analysis.title()
                        else:
                            print("Unknown string. Check your selection")
                    

                    if analysis == "Spending History":
                        try:
                            conn = utils.connect_to_db()

                            # Fetch currency codes
                            with conn.cursor() as cur:
                                cur.execute("SELECT DISTINCT account_id, currency_code FROM Accounts WHERE user_id = %s", (current_user,))
                                rows = cur.fetchall()

                            if rows:
                                currencies = list(row[1].strip(',') for row in rows)

                            for i, code in enumerate(currencies):
                                print(f"{i}. {code}")

                            input_currency = input("Select a currency: ")

                            # Checks if input_currency is digit and returns corresponding string
                            if input_currency.isdigit():
                                index = int(input_currency)
                                
                                if 0 <= index < len(currencies):
                                    currency = currencies[index]
                                    
                            else:
                                # If input_currency is in currencies, then return currency
                                if input_currency in currencies:
                                    # Converts analysis to upper case
                                    currency = input_currency.upper()
                                else:
                                    print("Unknown string. Check your selection")

                            if currency:
                                for row in rows:
                                    if row[1] == currency:
                                        account_id = row[0]

                            print("Please ensure that the start date is always before the end date else this function will not work")
                            start_date = input("Enter start date in this format(YYYY-MM-DD): ")
                            end_date = input("Enter end date in this format(YYYY-MM-DD): ")
                            
                            if current_user:
                                if end_date >= start_date:
                                    if account_id:
                                        result = users.get_spending_history(account_id, current_user, start_date, end_date)
                                        print(result)

                                        data = pd.DataFrame(result)
                                        data = data.set_index("Date")
                                        print(data)
                                    else:
                                        print("Please choose an account")
                                else:
                                   print("Please check your start date and end date")
                            else:
                                print("Please login")
                        except Exception as e:
                            logger.error(f"Unable to get spending history: {e}")
                            raise
                        finally:
                            utils.release_conn(conn)

                    # Generate account statement
                    elif analysis == "Account Statement":
                        try:
                            conn = utils.connect_to_db()

                            # Fetches details from accounts
                            with conn.cursor() as cur:
                                cur.execute("SELECT DISTINCT account_id, currency_code FROM Accounts WHERE user_id = %s",
                                            (current_user,))
                                rows = cur.fetchall()

                            if rows:
                                currencies = list(row[1].strip(',') for row in rows)

                            for i, code in enumerate(currencies):
                                print(f"{i}. {code}")

                            input_currency = input("Select a currency: ")

                            # Checks if input_currency is digit and returns corresponding string
                            if input_currency.isdigit():
                                index = int(input_currency)
                                
                                if 0 <= index < len(currencies):
                                    currency = currencies[index]
                                    
                            else:
                                # If input_currency is in currencies, then return currency
                                if input_currency in currencies:
                                    # Converts analysis to upper case
                                    currency = input_currency.upper()
                                else:
                                    print("Unknown string. Check your selection")

                            if currency:
                                for row in rows:
                                    if row[1] == currency:
                                        account_id = row[0]

                            print("Please ensure that the start date is always before the end date else this function will not work")
                            start_date = input("Enter start date in this format(YYYY-MM-DD): ")
                            end_date = input("Enter end date in this format(YYYY-MM-DD): ")

                            if current_user:
                                if end_date >= start_date:
                                    if account_id:
                                        result = users.get_spending_history(account_id, current_user, start_date, end_date)
                                        for history in result:
                                            x_values = list(history.keys())

                                        data = pd.DataFrame(result, index= x_values)
                                        print(data)
                                    else:
                                        print("Please select a currency")
                                else:
                                    print("Check the dates you entered correctly")
                            else:
                                print("Please login")
                        except Exception as e:
                            logger.error(f"Unable to egt account statement: {e}")
                            raise
                        finally:
                            utils.release_conn(conn)

                # Close Account
                case "12":
                    try:
                        # Connect to db
                        conn = utils.connect_to_db()

                        # Fetch user's currencies from account
                        with conn.cursor() as cur:
                            cur.execute("SELECT DISTINCT account_id, currency_code FROM Accounts WHERE user_id = %s",
                                        (current_user,))
                            rows = cur.fetchall()

                            all_currencies = list(row[1].strip(',') for row in rows)

                            for i, code in all_currencies:
                                print(f"{i}. {code}")

                            input_currency = input("Select a currency")

                            # Checks if from_currency is digit and returns corresponding code
                            if input_currency.isdigit():
                                index = int(input_currency)
                                
                                if 0 <= index < len(currencies):
                                    currency = currencies[index]

                            else:
                                # If from_currency is in currencies, then return from_currency
                                if input_currency in currencies:
                                    # Converts currency code to uppercase
                                    currency = input_currency.upper()
                                else:
                                    print("Unknown currency. Check your selection")

                            
                            # Getting account_id
                            for row in rows:
                                if row[1].strip(',') == currency:
                                    account_id = row[0]

                                account = users.Account(1, currency, True, 0)
                                result = account.close_account(account_id, current_user)

                                print(result)
                    except Exception as e:
                        logger.error(f"Unable to close account: {e}")
                        raise
                    finally:
                        utils.release_conn(conn)

                # Logout
                case "13":
                    choice = input("Choose yes if you want to logout. Otherwise, no: ")

                    if choice.lower() == "yes":
                        current_user = None
                    

                # Quit the program
                case "14":
                    choice = input("Choose yes if you want to quit. Otherwise, no : ")
                    
                    if choice.lower() == "yes":
                        sys.exit()         

if __name__ == "__main__":
    Bank_App()

# Godspeed
