import csv

import requests
import json

from src.utilities.logger import log

class WQSession(requests.Session):
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