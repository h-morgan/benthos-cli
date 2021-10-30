import database as db
import pandas as pd
import matplotlib.pyplot as plt
import utils
import click

pd.set_option('display.max_columns', 30)


@click.command()
@click.option('--ticker', prompt = 'Enter a ticker in uppercase', help='Ticker symbol used to calculate margin of safety price')
@click.option('--viz/--no-viz', default=False, help='Display graphs or not')
def start(ticker, viz):
    '''
    This is the main function of the project
    Connects to Postgres DB
    Retrieves stock fundamental data and runs value cals
    '''
    STOCK = ticker

    # Connect to postgres db
    conn = db.connect_db()

    # Get income statement relevant columns for stock of interest
    income_stmt_query = f"""SELECT
                                ticker,
                                report_date,
                                shares_basic, 
                                revenue, 
                                net_income,
                                gross_profit,
                                operating_expenses,
                                income_tax_benefit_net,
                                pretax_income_loss,
                                operating_income_loss

                            FROM 
                                f_income_stmts_annual
                            WHERE 
                                ticker = '{STOCK}'
                            ORDER BY
                                report_date;"""

    income_df_columns = ["Ticker", "Date", "Shares", "Revenue", "Net Income", "Gross Profit", "Operating Expenses", "Income Tax", "Income Before Tax", "Operating Income"]

    income_df = db.postgres_to_df(conn, income_stmt_query, income_df_columns)

    # Get balance sheet columns
    balance_stmt_query = f"""SELECT
                                total_equity,
                                long_term_debt,
                                short_term_debt
                            FROM 
                                f_balance_sheets_annual b
                            WHERE 
                                b.ticker = '{STOCK}'
                            ORDER BY report_date;"""
    balance_df_cols = ["Total Equity", "Debt", "Short Term Debt"]
    balance_df = db.postgres_to_df(conn, balance_stmt_query, balance_df_cols)

    # Get cash flow stmt columns
    cashflow_stmt_query = f"""SELECT
                                net_cash_operating_activities,
                                dividends_paid
                            FROM 
                                f_cashflow_annual c
                            WHERE 
                                c.ticker = '{STOCK}'
                            ORDER BY
                                report_date;"""
    cashflow_df_cols = ["Net Cash from Operating Act", "Dividends Paid"]
    cashflow_df = db.postgres_to_df(conn, cashflow_stmt_query, cashflow_df_cols)

    # Join 3 resultant dfs together 
    stock_df = pd.concat([income_df, balance_df, cashflow_df], axis=1, sort=False)

    # Replace NaN in dividends column with zero
    stock_df["Dividends Paid"] = stock_df["Dividends Paid"].fillna(0)

    stock_df["EPS"] = (stock_df["Net Income"] + stock_df["Dividends Paid"]).div(stock_df["Shares"], axis=0)

    stock_df[["Sales Per Share", "Equity Per Share", "Op. Cash Per Share"]] = stock_df[["Revenue", "Total Equity", "Net Cash from Operating Act"]].div(stock_df["Shares"], axis=0)

    # Create separate df contain health check numbers, or "moat" indicator values
    health_check_df = stock_df[["Date", "Revenue", "Sales Per Share", "Net Income", "EPS", "Total Equity", "Equity Per Share", "Net Cash from Operating Act", "Op. Cash Per Share"]].copy()

    # If no data for the stock, end the program
    if stock_df.empty:
        result = {
            'ticker': STOCK,
            'error_msg': f"Sorry, we don't have data yet for {STOCK}. Try another stock."
        }
        print(f"Sorry, we don't have data yet for {STOCK}. Try another stock.")
        return result

    # Get incremental growth rates at 1 yr, 3yr, 5yr, max
    print(health_check_df[["Date", "Revenue", "Net Income", "Total Equity", "Net Cash from Operating Act"]])
    sales_growth = utils.compound_growth_rates(health_check_df, ["Revenue", "Sales Per Share"])
    earnings_growth = utils.compound_growth_rates(health_check_df, ["Net Income", "EPS"])
    equity_growth = utils.compound_growth_rates(health_check_df, ["Total Equity", "Equity Per Share"])
    cash_growth = utils.compound_growth_rates(health_check_df, ["Net Cash from Operating Act", "Op. Cash Per Share"])

    # Combine growth rate numbers into one dataframe and re-order columns
    growth_df = pd.concat([sales_growth[["Num Years Ago", "Revenue Growth", "Sales Per Share Growth"]], 
                            earnings_growth[["Net Income Growth", "EPS Growth"]], 
                            equity_growth[["Total Equity Growth", "Equity Per Share Growth"]], 
                            cash_growth[["Net Cash from Operating Act Growth", "Op. Cash Per Share Growth"]]], axis=1, sort=False)
    growth_df_raw_vals = growth_df[["Num Years Ago", "Revenue Growth", "Net Income Growth", "Total Equity Growth", "Net Cash from Operating Act Growth"]].copy()
    print(growth_df_raw_vals)

    avg_equity_growth_rate = (growth_df["Total Equity Growth"].mean())
    print("Equity Growth Rate", avg_equity_growth_rate)

    default_PE = avg_equity_growth_rate * 2
    print("Default PE Ratio", default_PE)

    year_now = int(health_check_df.loc[health_check_df.index[-1], "Date"].year)
    year_1_ago = int(health_check_df.loc[health_check_df.index[-2], "Date"].year)
    year_3_ago = int(health_check_df.loc[health_check_df.index[-4], "Date"].year)
    year_5_ago = int(health_check_df.loc[health_check_df.index[-6], "Date"].year)
    year_max = int(health_check_df.loc[health_check_df.index[0], "Date"].year)

    #Sales growth NOT by share
    total_sales_now = health_check_df.loc[health_check_df.index[-1], "Revenue"]
    total_sales_1_ago = health_check_df.loc[health_check_df.index[-2], "Revenue"]
    total_sales_3_ago = health_check_df.loc[health_check_df.index[-4], "Revenue"]
    total_sales_5_ago = health_check_df.loc[health_check_df.index[-6], "Revenue"]
    total_sales_max = health_check_df.loc[health_check_df.index[0], "Revenue"]

    total_sales_1yr_growth = utils.get_growth(total_sales_now, total_sales_1_ago, (year_now - year_1_ago))
    total_sales_3yr_growth = utils.get_growth(total_sales_now, total_sales_3_ago, (year_now - year_3_ago))
    total_sales_5yr_growth = utils.get_growth(total_sales_now, total_sales_5_ago, (year_now - year_5_ago))
    total_sales_max = utils.get_growth(total_sales_now, total_sales_max, (year_now - year_max))

  #  print("NOT BY SHARE 1 Year:", total_sales_1yr_growth, "3 Year:", total_sales_3yr_growth, "5 Year:", total_sales_5yr_growth, "Max:", total_sales_max)

    # Calculate ROIC https://www.investopedia.com/terms/r/returnoninvestmentcapital.asp
    profit_now = income_df.loc[income_df.index[-1], "Gross Profit"]
    profit_1_ago = income_df.loc[income_df.index[-2], "Gross Profit"]
    profit_3_ago = income_df.loc[income_df.index[-4], "Gross Profit"]
    profit_5_ago = income_df.loc[income_df.index[-6], "Gross Profit"]
    profit_max = income_df.loc[income_df.index[0], "Gross Profit"]

    operating_exp_now = income_df.loc[income_df.index[-1], "Operating Expenses"]
    operating_exp_1_ago = income_df.loc[income_df.index[-2], "Operating Expenses"]
    operating_exp_3_ago = income_df.loc[income_df.index[-4], "Operating Expenses"]
    operating_exp_5_ago = income_df.loc[income_df.index[-6], "Operating Expenses"]
    operating_exp_max = income_df.loc[income_df.index[0], "Operating Expenses"]

    operating_income_now = income_df.loc[income_df.index[-1], "Operating Income"]
    operating_income_1_ago = income_df.loc[income_df.index[-2], "Operating Income"]
    operating_income_3_ago = income_df.loc[income_df.index[-4], "Operating Income"]
    operating_income_5_ago = income_df.loc[income_df.index[-6], "Operating Income"]
    operating_income_max = income_df.loc[income_df.index[0], "Operating Income"]

    tax_now = income_df.loc[income_df.index[-1], "Income Tax"]
    tax_1_ago = income_df.loc[income_df.index[-2], "Income Tax"]
    tax_3_ago = income_df.loc[income_df.index[-4], "Income Tax"]
    tax_5_ago = income_df.loc[income_df.index[-6], "Income Tax"]
    tax_max = income_df.loc[income_df.index[0], "Income Tax"]

    Income_before_tax_now = income_df.loc[income_df.index[-1], "Income Before Tax"]
    Income_before_tax_1_ago = income_df.loc[income_df.index[-2], "Income Before Tax"]
    Income_before_tax_3_ago = income_df.loc[income_df.index[-4], "Income Before Tax"]
    Income_before_tax_5_ago = income_df.loc[income_df.index[-6], "Income Before Tax"]
    Income_before_tax_max = income_df.loc[income_df.index[0], "Income Before Tax"]
    
    tax_rate_now = (tax_now/Income_before_tax_now)
    tax_rate_1yr = (tax_1_ago/Income_before_tax_1_ago)
    tax_rate_3yr = (tax_3_ago/Income_before_tax_3_ago)
    tax_rate_5yr = (tax_5_ago/Income_before_tax_5_ago)
    tax_rate_max = (tax_max/Income_before_tax_max)

    print(tax_rate_1yr)
    #First you need the NOPAT (Net Operating Profit After Tax) 
    #Formulas: NOPAT: https://www.investopedia.com/terms/n/nopat.asp 

    # Calculate the tax rate : provision tax/ Income before taxes
    # adding op exp to profit because it is a negative number in database
    nopat_now = (profit_now + operating_exp_now) * (1+tax_rate_now)
    nopat_1yr = (profit_1_ago + operating_exp_1_ago) * (1+tax_rate_1yr)
    nopat_3yr = (profit_3_ago + operating_exp_3_ago) * (1+tax_rate_3yr)
    nopat_5yr = (profit_5_ago + operating_exp_3_ago) * (1+tax_rate_5yr)
    nopat_max = (profit_max + operating_exp_max) * (1+tax_rate_max)

    nopat_now2 = operating_income_now * (1+tax_rate_now)
    nopat_1yr2 = operating_income_1_ago * (1+tax_rate_1yr)
    nopat_3yr2 = operating_income_3_ago * (1+tax_rate_3yr)
    nopat_5yr2 = operating_income_5_ago * (1+tax_rate_5yr)
    nopat_max2 = operating_income_max * (1+tax_rate_max)

    print("NOPAT: ", nopat_now, nopat_now2, nopat_1yr, nopat_1yr2, nopat_3yr, nopat_3yr2, nopat_5yr, nopat_5yr2)

    #Find debt and equity
    LT_debt_now = balance_df.loc[balance_df.index[-1], "Debt"]
    LT_debt_1yr = balance_df.loc[balance_df.index[-2], "Debt"]
    LT_debt_3yr = balance_df.loc[balance_df.index[-4], "Debt"]
    LT_debt_5yr = balance_df.loc[balance_df.index[-6], "Debt"]
    LT_debt_max = balance_df.loc[balance_df.index[0], "Debt"]

    ST_debt_now = balance_df.loc[balance_df.index[-1], "Short Term Debt"]
    ST_debt_1yr = balance_df.loc[balance_df.index[-2], "Short Term Debt"]
    ST_debt_3yr = balance_df.loc[balance_df.index[-4], "Short Term Debt"]
    ST_debt_5yr = balance_df.loc[balance_df.index[-6], "Short Term Debt"]
    ST_debt_max = balance_df.loc[balance_df.index[0], "Short Term Debt"]

    equity_now = balance_df.loc[balance_df.index[-1], "Total Equity"]
    equity_1yr = balance_df.loc[balance_df.index[-2], "Total Equity"]
    equity_3yr = balance_df.loc[balance_df.index[-4], "Total Equity"]
    equity_5yr = balance_df.loc[balance_df.index[-6], "Total Equity"]
    equity_max = balance_df.loc[balance_df.index[0], "Total Equity"]

    roic_now = utils.invested_capital(nopat_now, LT_debt_now, ST_debt_now, equity_now)
    roic_1yr = utils.invested_capital(nopat_1yr, LT_debt_1yr, ST_debt_1yr, equity_1yr)
    roic_3yr = utils.invested_capital(nopat_3yr, LT_debt_3yr, ST_debt_3yr, equity_3yr)
    roic_5yr = utils.invested_capital(nopat_5yr, LT_debt_5yr, ST_debt_5yr, equity_5yr)
    roic_max = utils.invested_capital(nopat_max, LT_debt_max, ST_debt_max, equity_max)

    print("ROIC Now: ", roic_now, "ROIC 1 YR: ", roic_1yr, "ROIC 3 YR: ", roic_3yr, "ROIC 5 YR: ", roic_5yr, "ROIC Max: ", roic_max)

    # TODO Plot the incremental growth numbers

    avg_equity_growth_rate = (growth_df["Total Equity Growth"].mean())
    print("Equity Growth Rate", avg_equity_growth_rate)

    default_PE = avg_equity_growth_rate * 2
    print("Default PE Ratio", default_PE)

    #FUTURE EPS
    NI_now = income_df.loc[income_df.index[-1], "Net Income"]
    shares_now = income_df.loc[income_df.index[-1], "Shares"]
    
    dividends_now = stock_df.loc[stock_df.index[-1], "Dividends Paid"]
    EPS_current = (NI_now+dividends_now)/shares_now
    print(stock_df["EPS"])
    print("Current EPS", EPS_current)

    # Equity growth rate
    print("Equity Growth Rate", avg_equity_growth_rate)
    # calc: https://www.symbolab.com/solver/calculus-calculator/%5Cleft(%5Cfrac%7Bx%7D%7B17.5428%7D%5Cright)%5E%7B%5Cfrac%7B1%7D%7B10%7D%7D%20-1%20%3D%20.19
    EPS_tenYrs_from_now = (((avg_equity_growth_rate/100)+1)**10) * EPS_current
    print("EPS 10 yrs from now", EPS_tenYrs_from_now)

    #Market Price
    future_mkt_price = EPS_tenYrs_from_now * default_PE
    print("Furture Market Price: ", future_mkt_price)
    sticker_price = future_mkt_price/4.0456 #rule of 72 here taking into account the 15% min acceptable rate of return
    MOS_sticker_price = sticker_price/2
    print("Stciker Price: ", sticker_price, "Margin of Safety Sticker Price: ", MOS_sticker_price)

    result = {
        'ticker': ticker,
        'sticker_price': round(sticker_price, 2),
        'safety_price': round(MOS_sticker_price, 2),
        'equity_growth': round(avg_equity_growth_rate, 2)
    }



    if viz:
        # Print raw numbers and per share values as a figure
        fig, axes = plt.subplots(nrows=4, ncols=2)
        health_check_df.plot(subplots=True, ax=axes, x="Date")
        fig.autofmt_xdate(rotation=45)
        fig.suptitle(f"{STOCK} Moat Indicators")
        plt.show()
        
        # Plot growth rate values
        fig2, axes2 = plt.subplots(nrows=4, ncols=2)
        growth_df.plot(subplots=True, ax=axes2, x="Num Years Ago")
        fig2.autofmt_xdate(rotation=45)
        fig2.suptitle(f"{STOCK} Growth Rates")
        plt.show()

    return result


def main(ticker):
    """This is the main method to act as a controller function"""

    # connect to db

    # get income statement data for stock

    # get balance sheet data for stock

    # get cash flow data for stock

    # join and clean returned data, compute additional data columns


    

if __name__ == "__main__":
    start()
