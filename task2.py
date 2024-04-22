from datetime import datetime, timedelta
from multiprocessing import Pool
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

CREDENTIALS = service_account.Credentials.from_service_account_file(
    "",  ## your path to service account key
)

def fetch_data(date):
    """
    Fetches data from a BigQuery table for a given date and filters the results 
    based on the maximum length of each row.

    Parameters:
        date (datetime.date): The date for which to fetch data.

    Returns:
        pandas.DataFrame: The filtered DataFrame containing the fetched data.
        (filter applied to remove rows with length greater than 50000,
        because google sheets can't handle more than 50000 characters)
    """
    client = bigquery.Client(credentials=CREDENTIALS)

    query = f"""
        SELECT *
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_{date.strftime('%Y%m%d')}`
    """

    # Run the query
    query_job = client.query(query)

    # Fetch the results into a pandas DataFrame
    df = query_job.to_dataframe()

    df_filtered = df[df.apply(lambda row: row.astype(str).apply(len).max() <= 50000, axis=1)]

    print(f"Data for {date.strftime('%Y-%m-%d')} has been fetched.")
    return df_filtered

def getanalytics_data(start, end):
    """
    Fetches data from BigQuery for the given date range and 
    concatenates the results into a single DataFrame.

    Parameters:
        start_date (datetime.date): The start date of the date range.
        end_date (datetime.date): The end date of the date range.

    Returns:
        pandas.DataFrame: The concatenated DataFrame containing the fetched data.
    """

    date_range = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    with Pool() as pool:
        data_frames = pool.map(fetch_data, date_range)
    all_data = pd.concat(data_frames, ignore_index=True)
    return all_data

def split_data_by_channel(all_data):
    """
    Split the given DataFrame `all_data` into separate DataFrames 
    based on the unique values in the "channelGrouping" column.
    
    Parameters:
        all_data (pandas.DataFrame): The DataFrame to be split.
        
    Returns:
        dict
    """
    data_frames = {}
    for channel, df in all_data.groupby("channelGrouping"):
        data_frames[channel] = df
    return data_frames

def apply_filter(df):
    """
    Apply a filter to the given DataFrame.

    Parameters:
        df (pandas.DataFrame): The DataFrame to apply the filter to.

    Returns:
        pandas.DataFrame: The filtered DataFrame.

    Description:
        This function applies a filter to the given DataFrame based on the following conditions:
        1. The 'pageviews' value in the 'totals' column is not None and is greater than 5.
        2. The 'timeOnSite' value in the 'totals' column is not None and is greater than 300.
        3. The length of the 'customDimensions' column is greater than 0.

        The function returns the filtered DataFrame.

    Example:
        >>> df = pd.DataFrame({'totals': [{'pageviews': 6, 'timeOnSite': 301}, 
        ... {'pageviews': 4, 'timeOnSite': 299}, 
        ... {'pageviews': None, 'timeOnSite': 305}], 
        ... 'customDimensions': [{'key': 'value'}, {}, {'key': 'value'}]})
        >>> apply_filter(df)
           totals customDimensions
        0  {'pageviews': 6, 'timeOnSite': 301}     {'key': 'value'}
        2  {'pageviews': None, 'timeOnSite': 305}     {'key': 'value'}
    """
    filtered_df = df[(df['totals'].apply(lambda x: x.get('pageviews', 0) is not None and x.get('pageviews', 0) > 5)) &
                    (df['totals'].apply(lambda x: x.get('timeOnSite', 0) is not None and x.get('timeOnSite', 0) > 300)) &
                    (df['customDimensions'].apply(lambda x: len(x) > 0))
                    ]
    return filtered_df

def export_to_google_sheets(df, sheet_name):
    """
    Export the given DataFrame to a Google Sheets spreadsheet.

    Parameters:
        df (pandas.DataFrame): The DataFrame to export.
        sheet_name (str): The name of the sheet to create or overwrite.

    Returns:
        None
    """
    gc = gspread.service_account(filename="") ## json keyfile
    sh = gc.open_by_key("14Mu45o65iAX9K8u2tEcCHke_MJfB9HyhZcRPbd3LWVA")
    worksheet = sh.add_worksheet(title=sheet_name, rows=1, cols=1)
    set_with_dataframe(worksheet, df)

    print(f"Filtered data has been exported to Google Sheets: {sheet_name}")

if __name__ == '__main__':
    ## Define the start and end dates
    start_date = datetime(2017, 7, 24).date()
    end_date = datetime(2017, 7, 31).date()

    # Fetch all the data for the week
    all_data = getanalytics_data(start_date, end_date)

    # Split the combined DataFrame into separate DataFrames based on "channelGrouping"
    channel_data_frames = split_data_by_channel(all_data)

    # Apply the filter to each DataFrame and export to Google Sheets
    for channel, df in channel_data_frames.items():
        filtered_df = apply_filter(df)
        export_to_google_sheets(filtered_df, f"google_analytics_data_{channel}_filtered")
