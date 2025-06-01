# Standard library imports
import csv
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, current_thread
from urllib.parse import urljoin

# Third-party imports
import pandas as pd
from filelock import FileLock

# Local application imports
from src.core.wq_session_core import WQSession
from src.utilities.logger import log





class WQSimulation(WQSession):
    """
    WQSimulation class for simulating alpha strategies on the WorldQuant platform.
    Handles simulation submission, monitoring, and result processing.
    """

    RESULT_CSV_HEADER = [
            'idea_id', 'status', 'passed_checks', 'failed', 'delay', 'region', 'neutralization', 'decay', 'truncation',
            'sharpe', 'fitness', 'turnover', 'weight', 'subsharpe', 'correlation',
            'universe', 'link', 'code', 'id'
        ]

    def __init__(self, json_fn='credentials.json'):
        super().__init__(json_fn)

        self.config = json_fn

        self.processed_simulations = []
        self.rows_failed = []
        self.rows_unprocessed = []
        self.results_data = []

        self.result_csv = None

        self._unflushed_row_count = 0
        self._flush_interval = 5

        self.write_lock = Lock()


    def run_simulation_from_tracker(self, tracker_path='alpha_tracking.csv'):
        """
        Run simulations for a list of alpha strategies.

        Args:
            tracker_path: Path to the CSV file containing alpha strategies

        Returns:
            Tuple of (results_df, unprocessed_simulations)
        """



        # Use file locking to prevent concurrent access issues
        lock_file = f"{tracker_path}.lock"
        file_lock = FileLock(lock_file, timeout=10)

        with file_lock:
            simulation_params = self._load_parameters_from_tracker(tracker_path)

        if not simulation_params:
            return

        self.processed_simulations = []
        self.rows_failed = []
        self.rows_unprocessed = []
        self.results_data = []

        timestamp = pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S-%f')
        csv_file = f"data/api_{timestamp}.csv"

        # Create CSV file with headers
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        self.result_csv = csv_file

        with open(csv_file, 'w', newline='') as f:
            self.f = f
            self.writer = csv.writer(f)

            self._write_result_row(self.RESULT_CSV_HEADER)

            # Use ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=3) as executor:
                # Submit individual simulation jobs directly to _process_simulation
                future_to_sim = {
                    executor.submit(self._process_simulation, sim): sim
                    for sim in simulation_params
                }

                # Process results as they complete
                for future in as_completed(future_to_sim):
                    sim = future_to_sim[future]
                    try:
                        result_row = future.result()
                        if result_row:
                            log.info(
                                f"{current_thread().name} -- Simulation completed for {sim.get('idea_id', 'UNKNOWN')}: {sim.get('code', '')[:20]}..."
                            )
                            self.update_tracker_with_results(tracker_path, result_row)

                    except Exception as e:
                        log.error(f"{current_thread().name} -- Error processing simulation {sim.get('code', 'UNKNOWN')[:20]}: {repr(e)}")
                        log.error(f"{current_thread().name} -- Full traceback:\n" + traceback.format_exc())


        log.info(f"Simulation completed!, {len(self.processed_simulations)} processed, {len(self.rows_failed)} failed")

    def _load_parameters_from_tracker(self, tracker_path):
        """
        Load simulation parameters from the tracker CSV file.
        This function reads the tracker file, filters for pending simulations,
        and marks them as processing to prevent concurrent access issues.
        It returns a list of dictionaries containing the simulation parameters.
        Each dictionary contains the
        parameters needed for the simulation, including 'code', 'neutralization',
        'decay', 'truncation', 'delay', 'universe', 'region', 'pasteurization',
        'nanHandling', and 'idea_id'.

        :param tracker_path: Path to the tracker CSV file
        :return: List of dictionaries containing simulation parameters
        """

        # Read the tracker file
        if not os.path.exists(tracker_path):
            log.info(f"Tracker file {tracker_path} not found")
            return

        df = pd.read_csv(tracker_path)
        pending_df = df[df['status'] == 'pending'].copy()

        if pending_df.empty:
            log.info("No pending alphas to simulate")
            return

        log.info(f"Found {len(pending_df)} pending alphas to simulate")

        # Convert to simulation parameters format
        simulation_params = []
        for _, row in pending_df.iterrows():
            sim_dict = {
                'code': row['code'],
                'neutralization': row.get('neutralization', 'SUBINDUSTRY'),
                'decay': row.get('decay', 0),
                'truncation': row.get('truncation', 0.05),
                'delay': row.get('delay', 1),
                'universe': row.get('universe', 'TOP3000'),
                'region': row.get('region', 'USA'),
                'pasteurization': row.get('pasteurization', 'ON'),
                'nanHandling': row.get('nanHandling', 'OFF'),
                'idea_id': row.get('idea_id', '')  # Track idea_id for updating
            }
            simulation_params.append(sim_dict)

        return simulation_params


    def _process_simulation(self, simulation):
        """Process a single alpha simulation"""
        # import random
        # time.sleep(random.uniform(1, 5)) # Added wait time to reduce API load
        if self.login_expired:
            return

        thread = current_thread().name

        # Extract parameters from simulation dictionary
        params = self._extract_simulation_params(simulation)
        alpha_code = params.pop('code').replace('"', '')
        alpha_code = alpha_code.replace(r"\r", "")
        alpha_code = alpha_code.replace(r"\n", "")

        log.info(f"{thread} -- Simulating alpha: {alpha_code[:50]}...")

        # Submit simulation
        simulation_link = self._submit_simulation(alpha_code, **params)
        if not simulation_link:
            return self._record_failed_simulation(simulation, params)

        # Monitor simulation
        alpha_link = self._monitor_simulation(simulation_link)
        if not alpha_link:
            return self._record_failed_simulation(simulation, params)

        # Process results
        return self._process_results(simulation, alpha_link, params)

    def _extract_simulation_params(self, simulation):
        """Extract parameters from simulation dictionary"""
        return {
            'code': simulation['code'].strip(),
            'delay': simulation.get('delay', 1),
            'universe': simulation.get('universe', 'TOP3000'),
            'truncation': simulation.get('truncation', 0.1),
            'region': simulation.get('region', 'USA'),
            'decay': simulation.get('decay', 6),
            'neutralization': simulation.get('neutralization', 'SUBINDUSTRY').upper(),
            'pasteurization': simulation.get('pasteurization', 'ON'),
            'nanHandling': simulation.get('nanHandling', 'OFF')
        }

    def _submit_simulation(self, alpha_code, **params):
        """Submit a simulation request to the WQ platform"""
        thread = current_thread().name

        payload = {
            'regular': alpha_code,
            'type': 'REGULAR',
            'settings': {
                "nanHandling": params.get('nanHandling'),
                "instrumentType": "EQUITY",
                "delay": int(params.get('delay')),
                "universe": params.get('universe'),
                "truncation": params.get('truncation'),
                "unitHandling": "VERIFY",
                "pasteurization": params.get('pasteurization'),
                "region": params.get('region'),
                "language": "FASTEXPR",
                "decay": int(params.get('decay')),
                "neutralization": params.get('neutralization'),
                "visualization": False
            }
        }

        url = urljoin(self.API_BASE_URL, "simulations")
        response = self.request_with_retry(self.post, url, json=payload)

        if not response or self.login_expired:
            log.error(f'{thread} -- Failed to submit simulation request: {response}; Login expired: {self.login_expired}')
            return None

        try:
            simulation_link = response.headers.get('Location')
            log.info(f'{thread} -- Obtained simulation link: {simulation_link}')
            return simulation_link
        except Exception as e:
            log.error(f'{thread} -- Failed to get simulation link: {str(e)}')
            return None

    def request_with_retry(self, method, url, max_attempts=3, **kwargs):
        """
        Retry a request with exponential backoff.

        Args:
            method: HTTP method function (e.g., self.get, self.post)
            url: URL to send the request to
            max_attempts: Maximum number of retry attempts
            **kwargs: Additional arguments to pass to the request method

        Returns:
            Response object or None if all attempts fail
        """
        thread = current_thread().name

        for attempt in range(1, max_attempts + 1):
            # 1. Check if we are in cooldown due to recent 429
            now = time.time()
            with self.rate_limit_lock:
                cooldown_left = self.cooldown_seconds - (now - self.last_429_time)
                if cooldown_left > 0:
                    log.warning(f"{thread} -- In cooldown from 429. Sleeping {cooldown_left:.1f}s before retrying...")
                    time.sleep(cooldown_left)

            self.rate_limiter.wait()
            try:
                response = method(url, **kwargs)

                # Check for credential/auth expiration
                auth_error = False
                try:
                    if response.status_code == 429:
                        with self.rate_limit_lock:
                            # Check again inside lock if this thread should initiate cooldown
                            now = time.time()
                            if now - self.last_429_time >= self.cooldown_seconds:
                                self.last_429_time = now
                                log.critical(
                                    f"{thread} -- 429 received. Entering global cooldown for {self.cooldown_seconds}s.")
                                time.sleep(self.cooldown_seconds)
                            else:
                                wait_remaining = self.cooldown_seconds - (now - self.last_429_time)
                                log.warning(
                                    f"{thread} -- 429 again. Respecting cooldown: waiting {wait_remaining:.1f}s.")
                                time.sleep(wait_remaining)
                        continue  # Retry the request after cooldown

                    if response.status_code == 400:
                        log.error(f"{thread} -- Bad request (400): {response.text}")
                        return None

                    if response.status_code in (401, 403):
                        auth_error = True
                        log.error(f"{thread} -- Bad request ({response.status_code}): {response.text}")

                    elif 'application/json' in response.headers.get('Content-Type', ''):
                        try:
                            detail = response.json().get('detail', '')
                            if 'credentials' in detail:
                                auth_error = True
                                log.error(f"{thread} -- Bad request ({response.status_code}): {response.text}")
                        except ValueError:
                            pass  # non-JSON body is fine
                except Exception as e:
                    log.warning(f"{thread} -- Error parsing response for auth: {str(e)}")

                if auth_error:
                    with self.login_lock:
                        if not self.login_expired:
                            log.warning(f"{thread} -- Detected login expiration, attempting to re-login...")
                            self.login_expired = True
                            login_success = self.login()
                            if login_success:
                                log.info(f"{thread} -- Re-login successful.")
                                self.login_expired = False
                                continue  # retry request
                            else:
                                log.error(f"{thread} -- Re-login failed.")
                                return None
                        else:
                            # Another thread is/was handling re-login
                            log.info(f"{thread} -- Login already marked as expired, skipping redundant re-login.")
                            return None

                return response  # success

            except Exception as e:
                log.warning(f"{thread} -- Request failed ({attempt}/{max_attempts}): {str(e)[:100]}")
                if attempt == max_attempts:
                    log.error(f"{thread} -- Max retry attempts reached for {url}")
                    return None

                sleep_time = min(2 ** attempt, 30)
                time.sleep(sleep_time)

        return None

    def _monitor_simulation(self, simulation_link, timeout=600):
        """Monitor simulation progress until completion or failure"""
        thread = current_thread().name

        start_time = time.time()

        while True:
            response = self.request_with_retry(self.get, simulation_link)
            if not response or self.login_expired:
                return None

            if time.time() - start_time > timeout:
                log.error(f"{thread} -- Simulation timed out after {timeout} seconds")
                return None

            try:
                data = response.json()

                # Check if simulation completed
                if 'alpha' in data:
                    alpha_link = data['alpha']
                    return alpha_link

                # Check for errors
                if 'message' in data and 'progress' not in data:
                    log.error(f"{thread} -- Simulation failed: {data['message']}")
                    return None

                # Log progress
                progress = int(100 * data['progress'])
                log.info(f"{thread} -- Waiting for simulation to end ({progress}%)")

                # Wait before checking again
                time.sleep(15)

            except Exception as e:
                log.error(f"{thread} -- Error monitoring simulation: {str(e)}")
                return None

    def _process_results(self, simulation, alpha_link, params):
        """Process simulation results and write to CSV"""
        thread = current_thread().name

        # Get full results
        url = urljoin(self.API_BASE_URL, f"alphas/{alpha_link}")
        response = self.request_with_retry(self.get, url)

        if not response or self.login_expired:
            return None

        try:
            data = response.json()
            platform_url = f"{self.PLATFORM_URL}/alpha/{alpha_link}"
            log.info(f'{thread} -- Obtained alpha link: {platform_url}')

            # Process check results
            passed, failed, weight_check, subsharpe = self._process_checks(data)

            # Create result row
            row = [
                simulation.get('idea_id', ''),
                'completed',
                passed,
                f'{",".join(failed)}',
                params['delay'],
                params['region'],
                params['neutralization'],
                params['decay'],
                params['truncation'],
                data['is']['sharpe'],
                data['is']['fitness'],
                round(100 * data['is']['turnover'], 2),
                weight_check,
                subsharpe,
                -1,
                params['universe'],
                platform_url,
                simulation['code'],
                alpha_link

            ]

            # Write row and mark as processed
            self._write_result_row(row)
            self.processed_simulations.append(simulation)
            self.results_data.append(row)  # Add to in-memory results
            log.info(f'{thread} -- Result added to CSV!')

            return dict(zip(self.RESULT_CSV_HEADER, row))

        except Exception as e:
            log.error(f"{thread} -- Error processing results: {str(e)}")

    def _record_failed_simulation(self, simulation, params):
        """Record a failed simulation in the CSV"""
        thread = current_thread().name
        log.info(f'{thread} -- Recording failed simulation')

        row = [
            simulation.get('idea_id', ''),
            'failed',
            0,
            '',
            params['delay'],
            params['region'],
            params['neutralization'],
            params['decay'],
            params['truncation'],
            0, 0, 0, 'FAIL', 0, -1,
            params['universe'],
            'FAILED',
            simulation['code'],
            ''
        ]

        self._write_result_row(row) # Write to CSV
        self.processed_simulations.append(simulation)
        self.rows_failed.append(row)  # Add to in-memory results
        log.warning(f'{thread} -- Simulation FAILED for {simulation.get('idea_id', '')}: {simulation["code"][:20]} – added to CSV')
        return dict(zip(self.RESULT_CSV_HEADER, row))

    def _write_result_row(self, row):
        with self.write_lock:
            self.writer.writerow(row)
            self._unflushed_row_count += 1

            if self._unflushed_row_count >= self._flush_interval:
                self.f.flush()
                self._unflushed_row_count = 0

    def _process_checks(self, data):
        """Process check results from simulation data"""
        passed = 0
        failed = []
        weight_check = 'UNKNOWN'
        subsharpe = -1

        for check in data['is']['checks']:
            if check['result'] == 'PASS':
                passed += 1
            elif check['result'] == 'FAIL':
                failed.append(check['name'])

            if check['name'] == 'CONCENTRATED_WEIGHT':
                weight_check = check['result']
            elif check['name'] == 'LOW_SUB_UNIVERSE_SHARPE':
                if check['result'] == 'ERROR':
                    subsharpe = 'ERROR'
                else:
                    subsharpe = check['value']

        return passed, failed, weight_check, subsharpe

    def update_tracker_with_results(self, tracker_path, result_row):
        """
        Update the tracker CSV with simulation results

        Args:
            tracker_path: Path to the tracker CSV
            result_row: A dictionary of results returned from _process_results()
        """
        thread = current_thread().name
        lock_file = f"{tracker_path}.lock"
        with FileLock(lock_file, timeout=20):
            df = pd.read_csv(tracker_path)

            idea_id = result_row.get('idea_id', '')
            match_index = df.index[df['idea_id'] == idea_id]

            if not match_index.empty:
                idx = match_index[0]

                # Update tracker with results
                df.loc[idx, 'status'] = result_row.get('status', 'error')
                df.loc[idx, 'passed_checks'] = result_row.get('passed_checks', 0)
                df.loc[idx, 'failed'] = result_row.get('failed', '')
                df.loc[idx, 'sharpe'] = result_row.get('sharpe', 0)
                df.loc[idx, 'fitness'] = result_row.get('fitness', 0)
                df.loc[idx, 'turnover'] = result_row.get('turnover', 0)
                df.loc[idx, 'weight_check'] = result_row.get('weight', '')
                df.loc[idx, 'subsharpe'] = result_row.get('subsharpe', 0)
                df.loc[idx, 'correlation'] = result_row.get('correlation', 0)
                df.loc[idx, 'link'] = result_row.get('link', '')
                df.loc[idx, 'id'] = result_row.get('id', '')

                df.to_csv(tracker_path, index=False)
                log.info(f"Updated tracker for {idea_id}")
            else:
                log.warning(f"Could not find tracker entry for idea_id={idea_id}")

        log.info(f"{thread} -- Successfully updated tracker entry for idea_id={idea_id}")
