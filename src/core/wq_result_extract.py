from pandas import json_normalize
import pandas as pd
import time
import os
from urllib.parse import urljoin

from src.core.wq_session_core import WQSession
from src.utilities.parameters import CREDENTIALS_FILE
from src.utilities.logger import log

class WQAlpha(WQSession):
    def __init__(self,alpha_id=None, json_fn=CREDENTIALS_FILE):
        super().__init__(json_fn=json_fn)
        self.base_url = 'https://api.worldquantbrain.com/alphas'
        self.alpha_id = alpha_id
        self.params = {
            'alpha_id': None,
            'recordset_id': None,
            'delay': 1
        }
        self.data = []
        self.data_set = None
        self.df_alpha = None
        self.unsubmitted_alphas_url = 'https://api.worldquantbrain.com/users/self/alphas?limit=10&offset=0&status!=SUBMITTED'
        self.submitted_alphas_url = 'https://api.worldquantbrain.com/users/self/alphas?limit=10&offset=0&status!=UNSUBMITTED'
        # self.get_data()

    def get_unsubmitted_alphas(self):
        r = self.get(self.unsubmitted_alphas_url, params=self.params)
        if r.status_code != 200:
            log.error(f"Error fetching data: {r.status_code}")
            return
        data = r.json()
        return data

    def get_submitted_alphas(self, csv_file_name=None):
        result = []
        url = self.submitted_alphas_url
        print(url)
        while url is not None and len(url) > 0:
            url = url.replace(':443', '')
            r = self.request_with_retry(self.get, url)

            data = r.json()
            df = json_normalize(data['results'])
            result.append(df)
            url = data.get('next', None)
            print(url)

        df_alpha = pd.concat(result)

        if csv_file_name is not None:
            df_alpha.to_csv(csv_file_name, index=False)

        self.df_alpha = df_alpha

        return df_alpha

    def get_submited_alpha_pnl(self, alpha_pnl_dir, df_alpha=None):
        if df_alpha is None and self.df_alpha is None:
            return None
        if df_alpha is None:
            df_alpha = self.df_alpha

        ls_pnl = []

        alpha_id = df_alpha.id

        for i in alpha_id:
            log.info(f'Extracting pnl for alpha {i}')
            alpha_pnl_path = os.path.join(alpha_pnl_dir, f'{i}.csv')
            if os.path.exists(alpha_pnl_path):
                df_pnl = pd.read_csv(alpha_pnl_path)
            else:
                df_pnl = self.get_single_alpha_pnl(i)
                df_pnl.to_csv(alpha_pnl_path, index=False)
            if df_pnl is not None:
                ls_pnl.append(df_pnl)

        log.info(f'{len(ls_pnl)} of {len(alpha_id)} alpha pnl are extracted')

        return pd.concat(ls_pnl)



    def get_single_alpha_pnl(self, alpha_id=None):
        if alpha_id is None:
            alpha_id = self.alpha_id
        url = f'{self.base_url}/{alpha_id}/recordsets/daily-pnl'

        pnl_df = self.get_single_alpha_result(url, alpha_id)

        return pnl_df
    def get_single_alpha_result(self, url, alpha_id):
        max_retries = 10

        while max_retries > 0:
            try:
                r = self.request_with_retry(self.get, url)
                if r.status_code != 200:
                    log.error(f"Error fetching data: {r.status_code}")
                    return None
                data = r.json()

                # Check if the response contains 'schema'
                if 'schema' not in data:
                    log.error(f"Error fetching data: {data}")

                if 'records' not in data:
                    log.error(f"Error fetching data: {data}")

                pnl_df = pd.DataFrame(data['records'])
                pnl_cols = [ r['name'] for r in data['schema']['properties']]
                pnl_df.columns = pnl_cols
                pnl_df['alpha_id'] = alpha_id
                return pnl_df
            except Exception as e:
                log.error(f"Error fetching data: {url} {e}")
            max_retries -= 1
            time.sleep(5)
        return None

    def get_single_alpha_performance_impact(self, url, alpha_id):
        max_retries = 10

        while max_retries > 0:
            try:
                r = self.request_with_retry(self.get, url)
                if r.status_code != 200:
                    log.error(f"Error fetching data: {r.status_code}")
                    return None
                data = r.json()

                performance_impact = data['score']['after'] - data['score']['before']
                df = pd.DataFrame([[alpha_id, performance_impact]], columns=['alpha_id', 'performance_impact'])
                return df
            except Exception as e:
                log.error(f"Error fetching data: {url} {e}")
            max_retries -= 1
            time.sleep(5)
        return None

    def cal_self_corr(self, df_submitted, df_new):
        # Check corr of new alpha with submitted alpha

        # Pivot submitted alphas to get alpha_id as columns and dates as index
        df_submitted_wide = df_submitted.pivot(index='date', columns='alpha_id', values='pnl')

        # Extract the new alpha pnl series
        new_alpha_id = df_new['alpha_id'].iloc[0]
        new_alpha_series = df_new.set_index('date')['pnl'].rename(new_alpha_id)

        # Ensure the new alpha is not in the submitted alphas
        df_submitted_wide = df_submitted_wide[[col for col in df_submitted_wide.columns if col != new_alpha_id]]

        # Align on dates to handle missing dates
        aligned = df_submitted_wide.join(new_alpha_series, how='inner')
        cutoff_date = pd.to_datetime(aligned.index[-1]) - pd.DateOffset(years=4)
        aligned_reduced = aligned[pd.to_datetime(aligned.index) >= cutoff_date].copy()

        # Calculate the correlation between the new alpha and each existing one
        correlations = aligned_reduced.corr()[new_alpha_id].drop([new_alpha_id])

        # Show the result
        return correlations

    def get_alpha_corr_and_performance(self, alpha_id, alpha=1):

        corr_url = f'https://api.worldquantbrain.com/alphas/{alpha_id}/correlations/self'
        perf_url = f'https://api.worldquantbrain.com/competitions/IQC2025S1/alphas/{alpha_id}/before-and-after-performance'

        corr_r = self.get_single_alpha_result(url=corr_url, alpha_id=alpha_id)
        perf_r = self.get_single_alpha_performance_impact(url=perf_url, alpha_id=alpha_id)
        high_corr = corr_r[corr_r['correlation']>0.7].copy()
        if len(high_corr) > 0:
            high_corr_high_sharpe = high_corr[high_corr['sharpe']*1.1 < alpha].copy()

        if perf_r is not None:
            improve_performance = f'Reduce performance by {perf_r['performance_impact'][0]}'
            if perf_r['performance_impact'][0] > 0:
                improve_performance = f'Improve performance by {perf_r['performance_impact'][0]}'
        else:
            improve_performance = 'Error on Performance Impact'

        if corr_r is not None:
            submittable = 'Cannot submit due to self-correlation'
            if len(high_corr) == 0 or (len(high_corr) > 0 and len(high_corr_high_sharpe) > 0):
                submittable = 'Can submit under self-correlation'
        else:
            submittable = 'Cannot submit due to error'
        return f'{improve_performance}; {submittable}'

    def corr_analysis(self, df_submitted_pnl, df_new_pnl, df_submitted_details, df_new_details=None):
        """
        Analyze the correlation of a new alpha with submitted alphas and find the one with highest correlation.
        :param df_submitted_pnl: DataFrame containing pnl of submitted alphas
        :param df_new_pnl: DataFrame containing pnl of the new alpha
        :param df_submitted_details: DataFrame containing details of submitted alphas
        :return: None, but prints the alpha with highest correlation and its Sharpe ratio
        """
        correlations = self.cal_self_corr(df_submitted_pnl, df_new_pnl)

        highest_corr = correlations.sort_values(ascending=False).iloc[0]
        over_threshold_alpha = correlations[correlations>0.7].copy()
        if len(over_threshold_alpha) == 0:
            highest_corr_alpha_id = correlations.sort_values(ascending=False).index[0]
            highest_corr_alpha_sharpe = df_submitted_details[
                df_submitted_details['id'] == highest_corr_alpha_id
            ]['is.sharpe'].iloc[0]
        else:
            over_threshold_alpha_sharpe = df_submitted_details[
                df_submitted_details['id'].isin(over_threshold_alpha.index)
            ].copy()
            highest_corr_alpha_sharpe = over_threshold_alpha_sharpe['is.sharpe'].max()
            highest_corr_alpha_id = over_threshold_alpha_sharpe[
                over_threshold_alpha_sharpe['is.sharpe']==highest_corr_alpha_sharpe
            ]['id'].iloc[0]

        max_retries = 5
        while max_retries > 0:
            try:
                if df_new_details is None:
                    log.info(f'No new alpha details provided, fetching details for {highest_corr_alpha_id}')
                    url = urljoin(self.API_BASE_URL, f"alphas/{highest_corr_alpha_id}")
                    response = self.request_with_retry(self.get, url)
                    sharpe = response.json()['is']['sharpe']
                else:
                    # Assume dataframe has 'id' and 'sharpe' columns
                    log.info(f'Fetching sharpe for {highest_corr_alpha_id} from provided details')
                    sharpe = df_new_details[df_new_details['id'] == df_new_pnl['alpha_id'].iloc[0]]['sharpe'].iloc[0]
                # exit loop if successful
                log.info(f'Highest correlation: {highest_corr}, Alpha ID: {highest_corr_alpha_id}, Sharpe: {sharpe}')
                break
            except Exception as e:
                log.error(f"Error fetching alpha details for {highest_corr_alpha_id}: {e}")
                sharpe = 0
            max_retries -= 1
            time.sleep(5)

        if highest_corr > 0.7 and sharpe / highest_corr_alpha_sharpe > 1.1:
            corr_check = True
        elif highest_corr < 0.7:
            corr_check = True
        else:
            corr_check = False

        return f'{corr_check}:{highest_corr}-{highest_corr_alpha_id}-{highest_corr_alpha_sharpe}'

if __name__ == '__main__':
    DATA_SET = WQAlpha()
    # DATA_SET.get_alpha_corr_and_performance('xxxxx')
    # result = DATA_SET.get_submitted_alphas('submitted_alphas.csv')
    # pnl = DATA_SET.get_submited_alpha_pnl(pd.read_csv('submitted_alphas.csv'))
    # pnl.to_csv('alpha_pnl.csv', index=False)
    # new_alpha = 'xxxx'
    # new_alpha_pnl = DATA_SET.get_single_alpha_pnl(new_alpha)
    # CORR = DATA_SET.cal_self_corr(pnl, new_alpha_pnl)