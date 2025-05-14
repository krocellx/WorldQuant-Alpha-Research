
from src.core.wq_simulation import WQSimulation


if __name__ == "__main__":
    TRACKER_FILE = 'alpha_tracking.csv'

    SIMULATOR = WQSimulation()
    SIMULATOR.run_simulation_from_tracker(TRACKER_FILE)
