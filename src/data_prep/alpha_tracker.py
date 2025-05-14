import csv
import pandas as pd

def create_tracking_csv(filename="alpha_tracking.csv"):
    """Create a CSV file with columns for tracking alpha strategies"""
    headers = [
        # Idea tracking
        'idea_id',
        'description',
        'hypothesis',
        'category',
        'creation_date',
        'status',  # e.g., "pending", "simulated", "submitted", "live"

        # Settings (input parameters)
        'code',
        'neutralization',
        'decay',
        'truncation',
        'delay',
        'universe',
        'region',
        'pasteurization',
        'nanHandling',

        # Results
        'passed_checks',
        'sharpe',
        'fitness',
        'turnover',
        'weight_check',
        'subsharpe',
        'correlation',
        'link',
        'notes'
    ]

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)

    return filename


# Example of adding a new idea to track
def add_new_idea(filename, idea_dict):
    """Add a new alpha idea to the tracking CSV"""
    with open(filename, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Extract values in the same order as headers
        row = [
            idea_dict.get('idea_id', ''),
            idea_dict.get('description', ''),
            idea_dict.get('hypothesis', ''),
            idea_dict.get('category', ''),
            idea_dict.get('creation_date', ''),
            idea_dict.get('status', 'pending'),

            idea_dict.get('code', ''),
            idea_dict.get('neutralization', 'MARKET'),
            idea_dict.get('decay', 0),
            idea_dict.get('truncation', 0.01),
            idea_dict.get('delay', 1),
            idea_dict.get('universe', 'TOP3000'),
            idea_dict.get('region', 'USA'),
            idea_dict.get('pasteurization', 'ON'),
            idea_dict.get('nanHandling', 'OFF'),

            idea_dict.get('passed_checks', ''),
            idea_dict.get('sharpe', ''),
            idea_dict.get('fitness', ''),
            idea_dict.get('turnover', ''),
            idea_dict.get('weight_check', ''),
            idea_dict.get('subsharpe', ''),
            idea_dict.get('correlation', ''),
            idea_dict.get('link', ''),
            idea_dict.get('notes', '')
        ]
        writer.writerow(row)

# Example of reading the CSV file
def generate_simulation_parameters(filename):
    """
    Read the alpha tracking CSV and generate simulation parameters.
    Parameter example:
        {
        'neutralization': 'MARKET',
        'decay': 4,
        'truncation': 0.08,
        'delay': 1,
        'universe': 'TOP3000',
        'region': 'USA',
        'code': 'trade_when(pcr_oi_270 < 1, (implied_volatility_call_270-implied_volatility_put_270), -1)'
    }
    :param filename: Path to the alpha tracking CSV file.
    :return:
    """
    df = pd.read_csv(filename)
    # Assuming you want to filter or process the DataFrame in some way
    # For example, filtering by status
    filtered_df = df[df['status'] == 'pending']

    # Convert to a list of dictionaries for easier processing
    simulation_params_columns = ['neutralization', 'decay', 'truncation', 'delay',
                                 'universe', 'region', 'code']

    simulation_parameters = []
    for _, row in filtered_df.iterrows():
        # Extract only the simulation-related parameters
        param_dict = {col: row[col] for col in simulation_params_columns if col in row}
        simulation_parameters.append(param_dict)

    return simulation_parameters

if __name__ == "__main__":
    # Example usage
    create_tracking_csv()
    print("Alpha tracking CSV created.")