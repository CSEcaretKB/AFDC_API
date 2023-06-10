import os
import keyring
import requests
import json


class API:
    def __init__(self):
        self.NAMESPACE = f"{os.environ.get('NAMESPACE')}_KEY"
        self.API_NAME = os.environ.get('API_NAME')
        self.base_url = 'https://developer.nrel.gov/api/alt-fuel-stations/v1.json'
        self.api_success_code = 200
        self.api_key = None
        self.json_dict = {}
        self.states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN',
                       'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ',
                       'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA',
                       'WI', 'WV', 'WY']
        self.validate_api_key()
        self.perform_api_request()

    def load_api_predicates(self, test=False, state=None):
        predicates = {
            'api_key': self.api_key,
            'status': 'all',
            'fuel_type': 'ELEC'
        }
        if test:
            predicates['limit'] = '1'
        if state:
            predicates['state'] = state

        return predicates

    def validate_api_key(self):
        while True:
            credential = keyring.get_credential(service_name=self.NAMESPACE, username=self.API_NAME)

            key_valid = False
            if credential:
                self.api_key = credential.password
                key_valid = self.test_api_key()

            if not key_valid:
                prompt = f'\n*** Please provide the API Key for the AFDC API ***\n'
                print(prompt)
                self.api_key = input('Enter here:')
                print(f'*** Testing API Key provided ***')
                key_valid = self.test_api_key()
                if key_valid:
                    print('*** API Key Validated! ***')
                    keyring.set_password(service_name=self.NAMESPACE, username=self.API_NAME, password=self.api_key)
                    break
                else:
                    print(f'\n*** Please provide the correct API Key for the AFDC API ***\n')
            else:
                break

    def test_api_key(self):
        api_test = requests.get(self.base_url,
                                params=self.load_api_predicates(test=True,
                                                                state=None))
        api_status = True if api_test.status_code == self.api_success_code else False
        return api_status

    def perform_api_request(self):
        print(f'\n--- PERFORMING API REQUESTS ---')
        for state in self.states:
            print(f'\tRequesting Data for {state}')
            response = requests.get(self.base_url,
                                    params=self.load_api_predicates(test=False,
                                                                    state=state))
            api_status = True if response.status_code == self.api_success_code else False
            if api_status:
                try:
                    self.json_dict[state] = json.loads(response.text)['fuel_stations']
                except ValueError:
                    print(f'\t\tState: {state} -- Returned no data.')
            else:
                print(f'\t\tState: {state} -- Returned a bad api status code.')
                print(f'\t\t{response.status_code}')
                print(f'\t\t{response.text}')
