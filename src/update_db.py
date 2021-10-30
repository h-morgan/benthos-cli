import pandas as pd
import psycopg2
import database as db
import os
import json
from alpha_vantage.fundamentaldata import FundamentalData
from datetime import datetime

token = os.environ.get("AV-API-TOKEN")

CURRENT_FY = 2020
# These are the options available to update db
TO_UPDATE_LIST = ["stock", "stocks"]
# This is the selection chosen to update
UPDATE = "stock"
STOCK = "AMAT"


def update_stocks_table(conn):
    '''
    This function takes a CSV of stock symbols named "companylist.csv"
    Must container columns Symbol, Name, Sector, Industry
    Writes them to the database
    '''

    # Open CSV file containing stock list
    dir = os.getcwd()
    update_file = dir + "/src/data/companylist.csv"

    try:
        new_data = pd.read_csv(update_file, sep=",")
    except (Exception, FileNotFoundError) as e:
        print("No stock csv found. Please save a file in the data/ dir called 'companylist.csv'.")
        return

    new_data['Symbol'] = new_data['Symbol'].str.replace('^','-')
    new_data['Name'] = new_data['Name'].str.replace('&#39;',"'")

    # Get the columns we are interested in and save to temp csv
    new_data = new_data[["Symbol", "Name", "Sector", "industry"]].copy()
    new_data = new_data.rename(columns={
                "Symbol":"ticker",
                "Name":"company_name",
                "Sector":"sector",
                "industry":"industry"})

    # Create a list of tuples from the DF
    tuples = [tuple(x) for x in new_data.to_numpy()]

    # Comma-separated df column names
    cols = ','.join(list(new_data.columns))
    table_name = 'd_stocks'

    # Craft the query
    query = f"""INSERT INTO {table_name} ({cols}) VALUES (%s,%s,%s,%s)"""

    # Create DB cursor
    cursor = conn.cursor()
    
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Error: {e}")
        conn.rollback()
        cursor.close()
        return 1
    print(f"Stocks added to table complete.")
    cursor.close()


def check_for_stock(conn, ticker):
    '''
    This function checks for the presence of ticker in our DB
    '''

    # Create SELECT query on d_stocks db table
    query = f"""SELECT ticker
                FROM d_stocks
                WHERE ticker = '{ticker}';"""
    
    cursor = conn.cursor()

    try:
        cursor.execute(query)
    
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Error performing query: {e}")
        cursor.close()
        return 1

    result = cursor.fetchone()
    cursor.close()
    
    if result is None: 
        return False
    
    else:
        return True



def get_updated_financials(conn, ticker):
    '''
    This function queries the Postgres DB for the ticker
    It receives the most recent year of data we have, and then retrieves most current
    years of data from Alpha Vantage
    '''

    # Query the income_statements table for the ticker in question
    query = f"""SELECT ticker, fiscal_year, report_date
                FROM f_income_stmts_annual
                WHERE ticker = '{ticker}'
                ORDER BY fiscal_year;"""
    
    # Convert query result to pandas df
    stock_report_dates = db.postgres_to_df(conn, query, ["ticker", "year", "report_date"])
    
    # If we don't have financial data yet, let the user know
    if stock_report_dates.empty:
        print(f"Sorry, we don't have any financial data yet for STOCK: {ticker}.")
        # TODO: create function to add NEW stock's data to database
        return None
    
    # If we do have the stock in our financial data, get the most recent year
    db_current = stock_report_dates.iloc[-1]["year"]
    current_report_date = stock_report_dates.iloc[-1]["report_date"]
    current_report_year = str(current_report_date.year)
    print(f"Most recent year we have {ticker} data for:", current_report_year, current_report_date)

    # Create fundamental data retrieval obj (alpha vantage)
    fd = FundamentalData(key=token, output_format="pandas")

    # Get income statement from alpha vantage
    income_stmts, _ = fd.get_income_statement_annual(symbol=ticker)
    balance_sheets, _ = fd.get_balance_sheet_annual(symbol=ticker)
    cash_stmts, _ = fd.get_cash_flow_annual(symbol=ticker)

    #print(balance_sheets.iloc[0:3].T)

    # Reset indices of all to be integers (come in as weird date/time index)
    income_stmts = income_stmts.reset_index()
    balance_sheets = balance_sheets.reset_index()
    cash_stmts = cash_stmts.reset_index()

    # Get index of row of most av df, correlating to most recent data we have in DB
    av_db_current = None
    for index, row in income_stmts.iterrows():
        year = row["fiscalDateEnding"][:4]
        # Store index of current data in alpha vantage df
        if year == current_report_year:
            print("Confirm current data year check (should match above): ", year, index)
            av_db_current = index
    
    # Get pandas df of just everything AFTER the most recent year we have data for
    income_stmts_new = income_stmts.iloc[0:av_db_current][:]
    balance_sheets_new = balance_sheets.iloc[0:av_db_current][:]
    cash_stmts_new = cash_stmts.iloc[0:av_db_current][:]

    # If any of these are empty DF's, that means we don't have new data to add
    if income_stmts_new.empty or balance_sheets_new.empty or cash_stmts_new.empty:
        print("No new data to add to DB, already up to date.")
        return

    length_income = len(income_stmts_new.index)
    length_balance = len(balance_sheets_new.index)
    length_cash = len(cash_stmts_new.index)

    # If each financial statement has the same amount of data to be added, we're good
    if length_income == length_balance and length_income == length_cash:

        data = {    
            "income_stmts" : income_stmts_new,
            "balance_sheets" : balance_sheets_new,
            "cash_stmts" : cash_stmts_new
        }
        return data
    # If not, don't add the data because that means we have an inconsistency somewhere
    else:
        print("Income statement, cashflow, and balance sheets contain different amounts of data to be added. \
            Aborting process.")
        return None


def get_var(row, map, db_key, alt_row=None):
    '''
    This function is used in add_financials_to_db function to retrieve 
    data from df and ensure it is in correct type and sign (+/-)
    Takes in the row from the AV dataframe, map (from json) of terms, and they db key name
    '''         

    # Get the value itself from the alpha vantage dataframe
    if alt_row is not None:
        value = alt_row[map[db_key]]
    else:
        value = row[map[db_key]]

    # Store list of keys that have need to be converted to negatives, non-integers
    negatives = ["cost_of_revenue", "operating_expenses", "selling_general_admin", "research_and_development", "interest_expense_net", "income_tax_benefit_net", "change_fixed_assets_intangibles"]
    non_int = ["currency"]

    if value == "None":
        return None

    if db_key in negatives:
        return -int(value)

    if db_key in non_int:
        return value

    else:
        return int(value)
    


def add_financials_to_db(conn, ticker, data):
    '''
    This function connects to the database and takes in financial stmt data to be added
    The `data` var is a dictionary containing keys: income_stmts, balance_sheets, cash_stmts
    Data from each of these is added to the respective tables in the db
    '''
    # Open a DB cursor
    cursor = conn.cursor()

    # Open json file containing mapping of Alpha Vantage terms to DB terms
    dir = os.getcwd()
    filename = dir + '/metadata/av_financial_map.json'
    f_open = open(filename)

    # Load mapping data as a Python dictionary
    json_map = json.load(f_open)
    income_map = json_map["income"]
    balance_map = json_map["balance"]
    cash_map = json_map["cash"]

    # Get each financial stmt as indepedent dictionary
    income_new = data["income_stmts"]
    balance_new = data["balance_sheets"]
    cash_new = data["cash_stmts"]

    # First deal with income stamement insert, create tuple of data to add to db
    # Loop over each row of new data (even though usually there will only be 1)
    for index, row in income_new.iterrows():
        # Convert report date to date object
        report_date = datetime.strptime(row[income_map["report_date"]], '%Y-%m-%d').date()
        # Craft list of values to enter, in sequence with DB columns (remember, shares comes from balance sheet)
        income_tuple = (ticker, None, get_var(row, income_map, "currency"), None, "FY", report_date, None, None, get_var(row, income_map, "shares_basic", balance_new.iloc[index]), None, get_var(row, income_map, "revenue"), get_var(row, income_map, "cost_of_revenue"), get_var(row, income_map, "gross_profit"), get_var(row, income_map, "operating_expenses"), get_var(row, income_map, "selling_general_admin"), get_var(row, income_map, "research_and_development"), None, get_var(row, income_map, "operating_income_loss"), get_var(row, income_map, "non_operating_income_loss"), get_var(row, income_map, "interest_expense_net"), None, None, get_var(row, income_map, "pretax_income_loss"), get_var(row, income_map, "income_tax_benefit_net"), get_var(row, income_map, "income_continuing_operations"), get_var(row, income_map, "net_extraordinary_gains_loss"), get_var(row, income_map, "net_income"), get_var(row, income_map, "net_income_common"))
        print("NEW INCOME STATEMENT VALS TO BE ADDED:")
        print(income_tuple)
        print(len(income_tuple))
        query = f"""INSERT INTO f_income_stmts_annual
                    VALUES %s"""
        cursor.execute(query, (income_tuple,))
        conn.commit()

    # Next deal with the balance sheet, do the same as above
    for index, row in balance_new.iterrows():
        report_date = datetime.strptime(row[balance_map["report_date"]], '%Y-%m-%d').date()
        balance_tuple = (ticker, None, get_var(row, balance_map, "currency"), None, "FY", report_date, None, None, get_var(row, balance_map, "cash_equiv_st_investmts"), get_var(row, balance_map, "accounts_notes_receivable"), get_var(row, balance_map, "inventories"), get_var(row, balance_map, "total_current_assets"), get_var(row, balance_map, "property_plant_equip_net"), get_var(row, balance_map, "long_term_invest_receivables"), get_var(row, balance_map, "other_long_term_assets"), get_var(row, balance_map, "total_noncurrent_assets"), get_var(row, balance_map, "total_assets"), get_var(row, balance_map, "payables_and_accruals"), get_var(row, balance_map, "short_term_debt"), get_var(row, balance_map, "total_current_liabilities"), get_var(row, balance_map, "long_term_debt"), get_var(row, balance_map, "total_noncurrent_liabilities"), get_var(row, balance_map, "total_liabilities"), get_var(row, balance_map, "share_cap_add_cap"), get_var(row, balance_map, "treasury_stock"), get_var(row, balance_map, "retained_earnings"), get_var(row, balance_map, "total_equity"), get_var(row, balance_map, "total_liabilities_and_equity"))
        print("NEW BALANCE STATEMENT VALS TO BE ADDED:")
        print(balance_tuple)
        print(len(balance_tuple))
        query = f"""INSERT INTO f_balance_sheets_annual
                    VALUES %s"""
        cursor.execute(query, (balance_tuple,))
        conn.commit()

    # Finally deal with the cash flow statement, same was as above
    for index, row in cash_new.iterrows():
        report_date = datetime.strptime(row[cash_map["report_date"]], '%Y-%m-%d').date()
        cash_tuple = (ticker, None, get_var(row, cash_map, "currency"), None, "FY", report_date, None, None, get_var(row, cash_map, "net_income_starting_line"), get_var(row, cash_map, "depreciation_and_amortization"), None, None, get_var(row, cash_map, "change_accts_receivable"), get_var(row, cash_map, "change_inventories"), get_var(row, cash_map, "change_accts_payable"), None, get_var(row, cash_map, "net_cash_operating_activities"), get_var(row, cash_map, "change_fixed_assets_intangibles"), get_var(row, cash_map, "net_change_long_term_invest"), None, get_var(row, cash_map, "net_cash_investing_activities"), get_var(row, cash_map, "dividends_paid"), get_var(row, cash_map, "cash_from_repay_debt"), get_var(row, cash_map, "cash_from_repurchase_equity"), get_var(row, cash_map, "net_cash_financing_activities"), get_var(row, cash_map, "net_change_cash"))
        print("NEW CASHFLOW STATEMENT VALS TO BE ADDED:")
        print(cash_tuple)
        print(len(cash_tuple))
        query = f"""INSERT INTO f_cashflow_annual
                    VALUES %s"""
        cursor.execute(query, (cash_tuple,))
        conn.commit()
    cursor.close()
        

def update():
    '''
    This is the main function of the update_db script
    Depending on user selection/input, service will be routed accordingly
    '''

    # Connect to postgres db
    conn = db.connect_db()

    if UPDATE == "none":
        print("Input UPDATE variable")
        return

    # Handle case where we are updating whole stock table in DB
    if UPDATE == "stocks":
        update_stocks_table(conn)

    # Handle individual stock financial statement updates
    if UPDATE == "stock" and STOCK is not None:
        stock_present = check_for_stock(conn, STOCK)

        # If stock is present, update each of the financial statements in DB
        if stock_present:
            print(f"{STOCK} is in our database. Continuing to financial statement udpates...")
            updated_financials = get_updated_financials(conn, STOCK)
            
            # If the financials retreived is None, end the process
            if updated_financials is None:
                print("No updated financial data was added to DB.")
                return

            # Otherwise, add the data retrieved to the database
            else:
                print(f"Retrieved {len(updated_financials['income_stmts'].index)} new year(s) of data for {STOCK}.")
                data_added = add_financials_to_db(conn, STOCK, updated_financials)       
            
        # If the stock is not in our DB, let the user now and terminate
        if stock_present is False:
            print(f"Sorry, STOCK: {STOCK} is not in our database yet.")




if __name__ == "__main__":
    update()