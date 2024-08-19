

import numpy as np
import pandas as pd
import requests
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Union, Optional


class DataHandler:
    def __init__(self, api_token_file: str):
        with open(api_token_file, 'r') as file:
            self.token = file.read().strip()
        self.base_url = 'https://api.tingo.com/tiingo/daily/'
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
        test_url = f"https://api.tingo.com/api/test?token={self.token}"
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
        params['startDate'] = start_date.isoformat() if start_date else None
        params['endDate'] = end_date.isoformat()
        if frequency:
            params['resampleFreq'] = frequency

        params = {k: v for k, v in params.items() if v is not None}
        return f"{self.base_url}{ticker}/prices?" + '&'.join(f"{k}={v}" for k, v in params.items())
    
    
    def get_historical_data(self, tickers: Union[str, List[str]], 
                            start_date: dt.date = dt.date(2000,1,1), end_date: Optional[dt.date] = None, 
                            frequency: str='daily', adj_close_only: bool =True) -> pd.DataFrame:
        """
        Retrieve historical data for on or more tickers.

        :param tickers: A single ticker of a list of tickers
        :param start_date: Start date for data retrieval
        :param end_date: End date for data retrieval (defaults to today)
        :param frequency: Data sampling frequency (e.g. 'daily', 'monthly')
        :param adj_close_only: If True, return only adjusted close prices for multiple tickers

        :return: DataFrame with historical data
        """
        if isinstance(tickers, str):
            tickers = [tickers]

        if end_date is None:
            end_date = dt.date.today()

        def fetch_data(ticker):
            url = self.construct_url(ticker, start_date, end_date, frequency)
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                df = pd.read_csv(pd.compat.StringIO(response.text))
                df['date'] = pd.to_datetime[df['date']]
                df.set_index('date', inplace=True)
                return ticker, df
            except requests.RequestException as e:
                print(f"Error fetching data for {ticker}: {e}")
                return ticker, None

            with ThreadPoolExecutor() as executor:
                results = list(executor.map(fetch_data, tickers))

            if len(tickers) == 1:
                return results[0][1]
            else:
                combined_df = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))
                for ticker, df in results:
                    if df is not None:
                        if adj_close_only:
                            combined_df[ticker] = df['adjClose']
                        else:
                            combined_df = pd.concat([combined_df, df], axis=1)
                return combined_df
            
    def get_returns(self, df: pd.DataFrame, column: Optional[Union[str, List[str]]] = None, log: bool = False) -> pd.DataFrame: 
        """
        Retrieves the normal or logarithmic returns of a DataFrame

        :param df: DataFrame containing tickers closing prices
        :param log: If True, returns the logarithmic return, otherwise returns normal returns
        :param column: Specific column to calculate returns for. If None, calculates for all columns

        :return: DataFrame of returns
        """
        if df.empty or len(df) < 2:
            raise ValueError("Input DataFrame is empty")
        
        if not np.issubdtype(df.dtypes.dtypes, np.number):
            raise ValueError("DataFrame contains non-numeric data")
        
        if columns is not None:
            if isinstance(columns, str):
                columns = [columns]
            df = df[columns]

        if log:
            returns_df = np.log(df / df.shift(1))
        else:   
            returns_df = (df - df.shift(1)) / df.shift(1)
        
        return returns_df

    
    



        
    
    
