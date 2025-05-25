
import pandas as pd

from src.utilities.logger import log

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
            backup_file = self.tracker_file.replace('.csv', '_backup.csv')
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
            print(f"Tracker file {self.tracker_file} not found. A new one will be created.")
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

            self.save_tracker()
        else:
            print(f"Idea ID {idea_id} not found in the tracker.")

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

if __name__ == "__main__":
    TRACKER = AlphaTracker(tracker_file=r'alpha\alpha_tracking_sample.csv')
    TRACKER.save_tracker()