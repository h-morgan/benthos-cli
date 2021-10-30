import pandas as pd

def get_growth(current, previous, n_years):
    '''
    This function takes two numbers and calculates the 
    compound growth rate over n years
    '''
    if current == previous:
        return 0

    if (current/previous) < 0:
        return float('inf')

    try:
        base = round(current/previous, 5)
        exponent = 1/n_years
        result = (((base**exponent)-1) * 100.0)
        return result
    except ZeroDivisionError:
        return float('inf')


def invested_capital(nopat, debt, st_debt, equity):

    try:
        # Do calculation
        invested_capital = debt + st_debt + equity
        return (nopat/invested_capital) * 100
    except ZeroDivisionError:
        return float('inf')

        
def compound_growth_rates(df, column_names):
    '''
    This function takes in a dataframe df with a list of given column_name's
    It calculates the 1 year, 3 year, 5 year, and max compound growth rates 
    over each period.
    Returns a df of two columns - [n_years, column_name]
    NOTE: df must contain a "Year" column
    '''
    # Initialize an empty dict to hold our column names, and resultant data
    result = {}

    # Get year value for each growth rate calculation
    year_now = int(df.loc[df.index[-1], "Date"].year)
    year_1_ago = int(df.loc[df.index[-2], "Date"].year)
    year_3_ago = int(df.loc[df.index[-4], "Date"].year)
    year_5_ago = int(df.loc[df.index[-6], "Date"].year)
    year_max = int(df.loc[df.index[0], "Date"].year)

    # Add Year and Num Years to dictionary
    result["Year"] = [year_1_ago, year_3_ago, year_5_ago, year_max]
    result["Num Years Ago"] = [(year_now-year_1_ago), (year_now-year_3_ago), (year_now-year_5_ago), (year_now-year_max)]
    
    # Loop over each column in column_names, complete calculations
    for col in column_names:

        # New col name
        new_name = col + " Growth"

        # Get values from column at now, 1, 3, 5, and max years ago
        vals_now = df.loc[df.index[-1], col]
        vals_1_ago = df.loc[df.index[-2], col]
        vals_3_ago = df.loc[df.index[-4], col]
        vals_5_ago = df.loc[df.index[-6], col]
        vals_max = df.loc[df.index[0], col]

        # Calculate compound growth rate at 1 year, 3, 5, and max years ago
        vals_1yr_growth = round(get_growth(vals_now, vals_1_ago, (year_now - year_1_ago)), 3)
        vals_3yr_growth = round(get_growth(vals_now, vals_3_ago, (year_now - year_3_ago)), 3)
        vals_5yr_growth = round(get_growth(vals_now, vals_5_ago, (year_now - year_5_ago)), 3)
        vals_growth_max = round(get_growth(vals_now, vals_max, (year_now - year_max)), 3)

        print(f"USED FOR {col} 3 YEAR GROWTH:", vals_now, vals_3_ago, (year_now - year_3_ago))

        # Add the resultant values to the results dict
        result[new_name] = [vals_1yr_growth, vals_3yr_growth, vals_5yr_growth, vals_growth_max]

    return pd.DataFrame(result)

