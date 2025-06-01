import csv
import time
from threading import Lock, current_thread
import threading

import requests
import json

from src.utilities.logger import log

class RequestRateLimiter:
    """
    A simple rate limiter to control the frequency of API requests.
    """
    def __init__(self, min_interval_sec=0.8):
        self.lock = threading.Lock()
        self.min_interval = min_interval_sec
        self.last_call = 0

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.time()

class WQSession(requests.Session):

    API_BASE_URL = "https://api.worldquantbrain.com"
    PLATFORM_URL = "https://platform.worldquantbrain.com"

    def __init__(self, json_fn='credentials.json'):
        super().__init__()
        # for handler in logging.root.handlers:
        #     logging.root.removeHandler(handler)
        # logging.basicConfig(encoding='utf-8', level=logging.INFO, format='%(asctime)s: %(message)s')
        self.json_fn = json_fn
        self.login()
        old_get, old_post = self.get, self.post
        def new_get(*args, **kwargs):
            try:    return old_get(*args, **kwargs)
            except: return new_get(*args, **kwargs)
        def new_post(*args, **kwargs):
            try:    return old_post(*args, **kwargs)
            except: return new_post(*args, **kwargs)
        self.get, self.post = new_get, new_post
        self.login_expired = False

        self.rate_limiter = RequestRateLimiter(min_interval_sec=5.0)

        self.login_lock = Lock()
        self.rate_limit_lock = Lock()
        self.last_429_time = 0
        self.cooldown_seconds = 60

    def login(self):
        with open(self.json_fn, 'r') as f:
            creds = json.loads(f.read())
            email, password = creds['email'], creds['password']
            self.auth = (email, password)
            r = self.post('https://api.worldquantbrain.com/authentication')
        if 'user' not in r.json():
            if 'inquiry' in r.json():
                input(f"Please complete biometric authentication at {r.url}/persona?inquiry={r.json()['inquiry']} before continuing...")
                self.post(f"{r.url}/persona", json=r.json())
            else:
                print(f'WARNING! {r.json()}')
                input('Press enter to quit...')
        log.info('Logged in to WQBrain!')
        return True

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


    def get_alpha_details(self, alpha_id):
        """
        Get details of a specific alpha by its ID.

        :param alpha_id: The ID of the alpha to retrieve details for
        :return: JSON response with alpha details or None if request fails
        """
        thread = current_thread().name
        url = f'https://api.worldquantbrain.com/alphas/{alpha_id}'

        response = self.request_with_retry(self.get, url)

        if response is None:
            log.error(f"{thread} -- Failed to retrieve alpha details for {alpha_id}")
            return None

        try:
            return response.json()
        except ValueError as e:
            log.error(f"{thread} -- Error parsing JSON response for alpha {alpha_id}: {str(e)}")
            return None