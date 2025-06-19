import os.path

import pandas as pd

from src.utilities.logger import log

from src.core.wq_result_extract import WQAlpha

class AlphaTracker:
    idea_cols = ['idea_id', 'description', 'hypothesis', 'category', 'template', 'operators', 'data', 'creation_date',
                 'status', 'note_1', 'note_2', 'manual_reviewed', 'submitted', 'last_updated']
    simulation_cols = ['code', 'neutralization', 'decay', 'truncation', 'delay', 'universe', 'region',
                       'pasteurization', 'nanHandling']
    result_cols = ['passed_checks', 'failed', 'sharpe', 'fitness', 'turnover', 'weight_check', 'subsharpe',
                   'correlation', 'link', 'id' ]
    all_cols = idea_cols + simulation_cols + result_cols

    col_types = {
        'idea_id': 'str',
        'description': 'str',
        'hypothesis': 'str',
        'category': 'str',
        'template': 'str',
        'operators': 'str',
        'data': 'str',
        'creation_date': 'datetime64[ns]',
        'status': 'str',
        'note_1': 'str',
        'note_2': 'str',
        'manual_reviewed': 'bool',
        'submitted': 'bool',
        'last_updated': 'datetime64[ns]',
        'code': 'str',
        'neutralization': 'str',
        'decay': 'int',
        'truncation': 'float',
        'delay': 'int',
        'universe': 'str',
        'region': 'str',
        'pasteurization': 'str',
        'nanHandling': 'str',
        'passed_checks': 'int',
        'failed': 'str',
        'sharpe': 'float',
        'fitness': 'float',
        'turnover': 'float',
        'weight_check': 'bool',
        'subsharpe': 'float',
        'correlation': 'float',
        'link': 'str',
        'id': 'str'
    }


    def __init__(self, tracker_file):
        self.tracker_file = tracker_file
        self.df = pd.DataFrame(columns=self.all_cols)
        self.load_tracker()

        self.default_simulation_params = {
        'neutralization': 'SUBINDUSTRY',
        'decay': 0,
        'truncation': 0.05,
        'delay': 1,
        'universe': 'TOP3000',
        'region': 'USA',
        'pasteurization': 'ON',
        'nanHandling': 'OFF'
    }

    def load_tracker(self):
        try:
            self.df = pd.read_csv(self.tracker_file)

            # Create backup of the original tracker file
            backup_file = self.tracker_file.replace('.csv', '.csv.backup')
            self.df.to_csv(backup_file, index=False)
            print(f"Tracker file {self.tracker_file} loaded successfully.")

            # Check for missing columns and add them if necessary
            for col in self.all_cols:
                if col not in self.df.columns:
                    self.df[col] = pd.Series(dtype=self.col_types[col])
                    print(f"Column {col} added to the tracker.")
                else:
                    # Convert to the specified type if the column exists
                    self.df[col] = self.df[col].astype(self.col_types[col], errors='ignore')
            for col in ['creation_date', 'last_updated']:
                if col in self.df.columns:
                    # Convert to datetime if the column exists
                    self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                else:
                    # Create the column with NaT values if it doesn't exist
                    self.df[col] = pd.NaT

            self.df['creation_date'] = pd.to_datetime(self.df['creation_date'])
            self.df['last_updated'] = pd.to_datetime(self.df['last_updated'])

            # order the columns
            self.df = self.df[self.all_cols]

        except FileNotFoundError:
            print(f"Tracker file {self.tracker_file} not found.")
        except pd.errors.EmptyDataError:
            print("Tracker file is currently empty or being written.")
        except pd.errors.ParserError:
            print("Tracker file is mid-write. Try again in a moment.")

    def append_tracker(self, new_data):
        # Ensure new_data is a DataFrame
        if not isinstance(new_data, pd.DataFrame):
            raise ValueError("new_data must be a pandas DataFrame")

        # Append new data to the existing tracker
        self.df = pd.concat([self.df, new_data], ignore_index=True)

        # Remove duplicates based on 'idea_id'
        self.df.drop_duplicates(subset=['idea_id'], keep='last', inplace=True)

    def save_tracker(self):
        df_temp = self.df.copy()
        df_temp['creation_date'] = df_temp['creation_date'].dt.strftime('%Y-%m-%d')
        df_temp['last_updated'] = df_temp['last_updated'].dt.strftime('%Y-%m-%d')
        df_temp.to_csv(self.tracker_file, index=False)
        print(f"Tracker file {self.tracker_file} updated successfully.")

    def update_idea(self, idea_id, key, value):
        if str(idea_id) in self.df['idea_id'].values:
            # Update the specified note column
            if key in self.df.columns:
                self.df.loc[self.df['idea_id'] == idea_id, key] = value
            else:
                print(f"Column {key} does not exist in the tracker.")

            self.df.loc[self.df['idea_id'] == idea_id, 'last_updated'] = pd.to_datetime('now')

        else:
            print(f"Idea ID {idea_id} not found in the tracker.")

    def update_idea_batch(self, idea_id, field_updates):
        """
        Update multiple fields for a single idea_id at once.

        Args:
            idea_id (str): The idea ID to update
            field_updates (dict): Dictionary of {field_name: new_value}

        Returns:
            bool: True if update succeeded, False otherwise
        """
        # Convert idea_id to string for consistent comparison
        idea_id = str(idea_id)

        # Find the row index first (more efficient)
        mask = self.df['idea_id'] == idea_id
        if not mask.any():
            print(f"Idea ID {idea_id} not found in the tracker.")
            return False

        # Get only valid columns that exist in the DataFrame
        valid_fields = {k: v for k, v in field_updates.items() if k in self.df.columns}
        if not valid_fields:
            print("No valid fields to update.")
            return False

        # Update all fields at once with a single loc operation
        self.df.loc[mask, list(valid_fields.keys())] = list(valid_fields.values())

        # Update timestamp
        self.df.loc[mask, 'last_updated'] = pd.to_datetime('now')
        return True

    def load_parameters(self):
        # Load parameters from the tracker file
        pending_df = self.df[self.df['status'] == 'pending']

        if pending_df.empty:
            log.info("No pending alphas to simulate")
            return []

        log.info(f"Found {len(pending_df)} pending alphas to simulate")

        # Convert to simulation parameters format
        simulation_params = []
        for _, row in pending_df.iterrows():
            sim_dict = {
                'code': row['code'],
                'neutralization': row.get('neutralization', 'SUBINDUSTRY'),
                'decay': int(row.get('decay', 0)),
                'truncation': row.get('truncation', 0.05),
                'delay': int(row.get('delay', 1)),
                'universe': row.get('universe', 'TOP3000'),
                'region': row.get('region', 'USA'),
                'pasteurization': row.get('pasteurization', 'ON'),
                'nanHandling': row.get('nanHandling', 'OFF'),
                'idea_id': row.get('idea_id', '')  # Track idea_id for updating
            }
            simulation_params.append(sim_dict)

        return simulation_params

    def corr_check(self, df_alpha_pnl_dir, df_submitted_pnl=None, df_submitted_details=None, sharpe_threshold=1.3, submitted_alpha_details_file_path=None):
        """
        Check correlation of alphas in the tracker.
        This is a placeholder for the actual correlation check logic.
        """
        log.info(f"Starting correlation check from alpha pnl dir: {df_alpha_pnl_dir}...")
        in_scope_alphas = self.df[(self.df['sharpe'] >= sharpe_threshold)
                                  # & (self.df['correlation'] == -1.0)
                                  & (self.df['submitted'] == False)].copy()
        if in_scope_alphas.empty:
            log.info("No alphas in scope for correlation check.")
            return True

        result_obj = WQAlpha()

        if df_submitted_details is None:
            df_submitted_details = result_obj.get_submitted_alphas(submitted_alpha_details_file_path)

        if df_submitted_pnl is None:
            df_submitted_pnl = result_obj.get_submited_alpha_pnl(df_alpha_pnl_dir, df_submitted_details)

        for _, row in in_scope_alphas.iterrows():
            idea_id = row['idea_id']
            code = row['code']
            alpha_id = row['id']
            log.info(f"Checking correlation for idea {idea_id} with code: {code[:50]}...")

            alpha_pnl_file_path = os.path.join(df_alpha_pnl_dir, f"{alpha_id}.csv")

            if os.path.isfile(alpha_pnl_file_path):
                log.info(f'Loading existing Alpha PnL file for {alpha_id} from {alpha_pnl_file_path}...')
                new_alpha_pnl = pd.read_csv(alpha_pnl_file_path)
            else:
                log.info(f'Alpha PnL file for {alpha_id} not found ({alpha_pnl_file_path}). Generating new PnL data...')
                new_alpha_pnl = result_obj.get_single_alpha_pnl(alpha_id)
                new_alpha_pnl.to_csv(alpha_pnl_file_path, index=False)

            corr_result = result_obj.corr_analysis(df_submitted_pnl,
                                                   new_alpha_pnl,
                                                   df_submitted_details,
                                                   row.to_frame().T[['sharpe', 'id']])

            self.update_idea(idea_id, 'correlation', corr_result)
            self.save_tracker()

        return True

    def check_correlation_between_alphas(self, alpha_ids=None, idea_ids=None, df_alpha_pnl_dir=None):
        """
        Check correlation between specified alphas.

        Args:
            alpha_ids (list): List of alpha IDs to check correlation between
            idea_ids (list): List of idea IDs to check correlation between (alternative to alpha_ids)
            df_alpha_pnl_dir (str): Directory containing alpha PnL CSV files

        Returns:
            pd.DataFrame: Correlation matrix between the specified alphas
        """
        if df_alpha_pnl_dir is None:
            raise ValueError("Alpha PnL directory must be specified")

        result_obj = WQAlpha()

        # Get alpha IDs from idea IDs if provided
        if alpha_ids is None and idea_ids is not None:
            alpha_ids = self.df[self.df['idea_id'].isin(idea_ids)]['id'].tolist()

        if not alpha_ids:
            log.info("No alphas specified for correlation check")
            return pd.DataFrame()

        # Load PnL data for each alpha
        pnl_data = {}
        for alpha_id in alpha_ids:
            alpha_pnl_file_path = os.path.join(df_alpha_pnl_dir, f"{alpha_id}.csv")

            if os.path.isfile(alpha_pnl_file_path):
                log.info(f'Loading Alpha PnL file for {alpha_id}...')
                alpha_pnl = pd.read_csv(alpha_pnl_file_path)
                pnl_data[alpha_id] = alpha_pnl
            else:
                log.info(f'Alpha PnL file for {alpha_id} not found. Generating new PnL data...')
                alpha_pnl = result_obj.get_single_alpha_pnl(alpha_id)
                alpha_pnl.to_csv(alpha_pnl_file_path, index=False)
                pnl_data[alpha_id] = alpha_pnl

        # Calculate correlation matrix
        # Extract the 'pnl' column from each DataFrame and create a new DataFrame with alpha_ids as column names
        pnl_df = pd.DataFrame()
        for alpha_id, df in pnl_data.items():
            if 'pnl' in df.columns:
                pnl_df[alpha_id] = df['pnl']

        # Calculate correlation matrix
        corr_matrix = pnl_df.corr()

        return corr_matrix

    def performance_check(self, sharpe_threshold=1.3, corr_verify=True):
        """
        Check performance of alphas in the tracker.
        This is a placeholder for the actual performance check logic.
        """
        log.info("Starting performance check...")

        in_scope_alphas = self.df[
            (self.df['sharpe'] >= sharpe_threshold) &
            (self.df['submitted'] == False)].copy()

        if corr_verify:
            log.info("Filter alphas with correlation check passed...")
            in_scope_alphas = in_scope_alphas[
                in_scope_alphas['correlation'].str.contains('True')]

        if in_scope_alphas.empty:
            log.info("No alphas in scope for performance check.")
            return True

        result_obj = WQAlpha()

        for _, row in in_scope_alphas.iterrows():
            idea_id = row['idea_id']
            code = row['code']
            alpha_id = row['id']
            log.info(f"Checking performance for idea {idea_id} with code: {code[:50]}...")

            perf_url = f'https://api.worldquantbrain.com/competitions/IQC2025S1/alphas/{alpha_id}/before-and-after-performance'
            perf_result = result_obj.get_single_alpha_performance_impact(
                url=perf_url,alpha_id=alpha_id)
            # Perform performance checks here
            # For example, you can check if the alpha meets certain criteria
            # and update the 'status' or other fields accordingly

            # Placeholder for actual performance check logic
            result_text = f'Pref: {perf_result['performance_impact'][0]};'
            self.update_idea(idea_id, 'note_2', result_text)
            self.save_tracker()

        return True



if __name__ == "__main__":
    TRACKER = AlphaTracker(tracker_file=r'E:\OneDrive\DataStorage\iqc_alpha\ts_mean_round_02.csv')
    # TRACKER.save_tracker()
    DF_SUBMITTED_PNL = pd.read_csv(r'E:\OneDrive\DataStorage\iqc_alpha\alpha_pnl.csv')
    DF_SUBMITTED_DETAILS = pd.read_csv(r'E:\OneDrive\DataStorage\iqc_alpha\submitted_alphas.csv')
    TRACKER.corr_check(df_alpha_pnl_dir=r'E:\OneDrive\DataStorage\iqc_alpha\pnl',
                       # df_submitted_pnl=DF_SUBMITTED_PNL,
                       df_submitted_details=DF_SUBMITTED_DETAILS, # DF_SUBMITTED_DETAILS,
                       submitted_alpha_details_file_path=r'E:\OneDrive\DataStorage\iqc_alpha\submitted_alphas.csv'
                       )
    # TRACKER.check_correlation_between_alphas(['29nR5Q5', '98AajNx', 'px1n0Xb', 'ENrrKVJ', 'xVmMqVb'], df_alpha_pnl_dir=r'E:\OneDrive\DataStorage\iqc_alpha\pnl')
    print(1)
