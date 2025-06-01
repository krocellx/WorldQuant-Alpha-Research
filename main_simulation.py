import os
from src.core.wq_simulation import WQSimulation
from src.utilities.logger import log

if __name__ == "__main__":
    input_txt = os.path.join('alpha_onedrive','input_filename.txt')
    with open(input_txt, 'r') as f:
        TRACKER_FILE = f.readline().strip()

    if not TRACKER_FILE:
        raise ValueError("No file name found in input_filename.txt")

    # TRACKER_FILE = 'behavioral_finance_round_1.csv'
    log.info(f"Running simulation with tracker file: {TRACKER_FILE}")
    SIMULATOR = WQSimulation()
    SIMULATOR.run_simulation_from_tracker(os.path.join('alpha_onedrive', TRACKER_FILE))
