

import numpy as np
import pandas as pd
import requests
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Union, Optional
from io import StringIO
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataHandler:
    def __init__(self, api_token_file: str):
        with open(api_token_file, 'r') as file:
            self.token = file.read().strip()
        self.base_url = 'https://api.tiingo.com/tiingo/daily/'
        self.default_params = {
            'format': 'csv',
            'resampleFreq': 'daily'
        }
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Token {self.token}'
        }


    def test_connections(self):
        """
        Test the API connection.
        """
        test_url = f"https://api.tiingo.com/api/test?token={self.token}"
        try:
            response = requests.get(test_url, headers=self.headers)
            response.raise_for_status()
            print("Connection successful:", response.json())
        except requests.RequestException as e:
            print(f"Connection failed: {e}")


    def construct_url(self, ticker: str, start_date: Optional[dt.date] = None, 
                      end_date: Optional[dt.date] = None, frequency: Optional[str] = None) -> str:
        """
        Construct the API URL for a given ticker and parameters.
        """
        if end_date is None:
            end_date = dt.date.today()
        params = self.default_params.copy()
        params['startDate'] = start_date if start_date else None
        params['endDate'] = end_date
        if frequency:
            params['resampleFreq'] = frequency

        params = {k: v for k, v in params.items() if v is not None}
        return f"{self.base_url}{ticker}/prices?" + '&'.join(f"{k}={v}" for k, v in params.items())
    

    


    def get_historical_data(self, tickers: Union[str, List[str]], 
                            start_date: Union[str, dt.date] = dt.date(2000,1,1), 
                            end_date: Optional[Union[str, dt.date]] = None, 
                            frequency: str='daily', adj_close_only: bool =True) -> pd.DataFrame:
        if isinstance(tickers, str):
            tickers = [tickers]

        if end_date is None:
            end_date = dt.date.today()

        start_date = start_date.strftime('%Y-%m-%d') if isinstance(start_date, dt.date) else start_date
        end_date = end_date.strftime('%Y-%m-%d') if isinstance(end_date, dt.date) else end_date

        def fetch_data(ticker):
            url = self.construct_url(ticker, start_date, end_date, frequency)
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                df = pd.read_csv(StringIO(response.text))
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                logger.info(f"Successfully fetched data for {ticker}")
                return ticker, df
            except requests.RequestException as e:
                logger.error(f"Error fetching data for {ticker}: {e}")
                return ticker, None
            except Exception as e:
                logger.error(f"Unexpected error for {ticker}: {e}")
                return ticker, None

        with ThreadPoolExecutor() as executor:
            results = list(executor.map(fetch_data, tickers))

        if len(tickers) == 1:
            df = results[0][1]
            if adj_close_only and df is not None:
                return df[['adjClose']]
            return df
        else:
            combined_df = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))
            for ticker, df in results:
                if df is not None:
                    if adj_close_only:
                        combined_df[ticker] = df['adjClose']
                    else:
                        combined_df = pd.concat([combined_df, df], axis=1)
            
            if combined_df.empty:
                logger.warning("No data was successfully retrieved for any ticker.")
            else:
                logger.info(f"Successfully retrieved data for {combined_df.shape[1]} tickers.")
            
            return combined_df
            
    def get_returns(self, df: pd.DataFrame, columns: Optional[Union[str, List[str]]] = None, log: bool = False) -> pd.DataFrame: 
        """
        Retrieves the normal or logarithmic returns of a DataFrame

        :param df: DataFrame containing tickers closing prices
        :param log: If True, returns the logarithmic return, otherwise returns normal returns
        :param column: Specific column to calculate returns for. If None, calculates for all columns

        :return: DataFrame of returns
        """
        if df.empty or len(df) < 2:
            raise ValueError("Input DataFrame is empty or has insufficient data points")
    
        # If columns is specified, select only those columns
        if columns is not None:
            if isinstance(columns, str):
                columns = [columns]
            df = df[columns]
        
        # Check if all remaining columns are numeric
        non_numeric_cols = df.select_dtypes(exclude=[np.number]).columns
        if len(non_numeric_cols) > 0:
            raise ValueError(f"DataFrame contains non-numeric data in columns: {list(non_numeric_cols)}")

        if log:
            returns_df = np.log(df / df.shift(1))
        else:   
            returns_df = df.pct_change()
        
        return returns_df.set_index(df.index)

    
    



        
    
    
