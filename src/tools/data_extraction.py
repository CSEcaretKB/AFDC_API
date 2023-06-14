import geopandas as gpd
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class DataExtractor:
    def __init__(self, json_dict):
        self.json_dict = json_dict
        self.states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN',
                       'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ',
                       'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA',
                       'WI', 'WV', 'WY']
        self.station_cols = ['access_code', 'status_code', 'open_date', 'station_name', 'street_address', 'city',
                             'state', 'zip', 'latitude', 'longitude', 'ev_network', 'owner_type_code',
                             'ev_level1_evse_num', 'ev_level2_evse_num', 'ev_dc_fast_num', 'ev_connector_types',
                             'ev_pricing', 'ev_renewable_source', 'geocode_status', 'facility_type',
                             'maximum_vehicle_class', 'expected_date', 'date_last_confirmed', 'updated_at']
        self.evse_cols = ['id', 'station_id', 'evse_id']
        self.network_ids = 'ev_network_ids'
        self.crs = 'EPSG:4326'
        self.station_data = {}
        self.evse_data = {}
        self.station_df = None
        self.evse_df = None

        self.extract_data()
        self.convert_to_dataframes()
        self.format_dataframes()

    def extract_data(self):
        print(f'\n--- EXTRACTING DATA FROM JSON RESPONSES ---')

        # Instantiate indices for inserting data into the station_data and evse_data dictionaries
        ix = 0
        iix = 0

        for state, record_list in self.json_dict.items():
            print(f'\tExtracting Data for {state}')
            for record in record_list:
                id = record['id']

                self.station_data[ix] = {}
                for col, value in record.items():
                    if col in self.station_cols:
                        self.station_data[ix][col] = value
                    elif col == 'id':
                        self.station_data[ix][col] = id
                    elif col == self.network_ids:
                        station_id = None
                        if record[self.network_ids].get('station'):
                            if len(record[self.network_ids]['station']) == 1:
                                station_id = record[self.network_ids]['station'][0]

                        evse_id = None
                        if record[self.network_ids].get('posts'):
                            for evse_id in record[self.network_ids]['posts']:
                                self.evse_data[iix] = {}
                                self.evse_data[iix]['id'] = id
                                self.evse_data[iix]['station_id'] = station_id
                                self.evse_data[iix]['evse_id'] = evse_id
                                # Add 1 to evse data index
                                iix += 1
                    else:
                        pass
                # Add 1 to station data index
                ix += 1

    def convert_to_dataframes(self):
        self.station_df = pd.DataFrame.from_dict(data=self.station_data,
                                                 columns=['id'] + self.station_cols,
                                                 orient='index').astype(str)

        self.evse_df = pd.DataFrame.from_dict(data=self.evse_data,
                                              columns=self.evse_cols,
                                              orient='index').astype(str)

    def format_dataframes(self):
        print(f'\n--- FORMATTING DATA IN STATION AND EVSE DATAFRAMES ---')
        # Replace None values and 'nan' strings with NaN
        self.evse_df = self.evse_df.replace(to_replace=[None, 'nan', 'None'],
                                            value=np.nan)

        # Replace None values and 'nan' strings with NaN
        self.station_df = self.station_df.replace(to_replace=[None, 'nan', 'None'],
                                                  value=np.nan)

        # State Name replacement
        self.station_df = self.state_name_formatting(dataframe=self.station_df,
                                                     df_col='state')

        # Status Code replacement
        self.station_df = self.status_code_formatting(dataframe=self.station_df,
                                                      df_col='status_code')

        # EVSP replacement
        self.station_df = self.ev_networking_formatting(dataframe=self.station_df,
                                                        df_col='ev_network')

        # Owner Type Code replacement
        self.station_df = self.owner_type_formatting(dataframe=self.station_df,
                                                     df_col='owner_type_code')

        # Convert dates
        date_cols = ['open_date', 'expected_date', 'date_last_confirmed']
        for col in date_cols:
            self.station_df[col] = pd.to_datetime(self.station_df[col],
                                                  infer_datetime_format=True,
                                                  errors='coerce')

        # Convert Datetime to UTC
        self.station_df['updated_at'] = pd.to_datetime(self.station_df['updated_at'],
                                                       infer_datetime_format=True,
                                                       errors='coerce').dt.tz_convert('UTC')

        # Convert strings to float
        float_cols = ['latitude', 'longitude']
        for col in float_cols:
            self.station_df[col] = pd.to_numeric(self.station_df[col],
                                                 errors='coerce',
                                                 downcast='float')

        # EVSE Number float conversion and fillna with 0
        fillna_cols = ['ev_level1_evse_num', 'ev_level2_evse_num', 'ev_dc_fast_num']
        for col in fillna_cols:
            self.station_df[col] = pd.to_numeric(self.station_df[col],
                                                 errors='coerce',
                                                 downcast='float')
            self.station_df[col] = self.station_df[col].fillna(0).astype(int)

        # Concatenate list values to ";" separated string
        concat_cols = ['ev_connector_types']
        for col in concat_cols:
            self.station_df[col] = self.station_df[col].astype(str)
            self.station_df[col] = self.station_df[col].str.replace('\W', ' ', regex=True)
            for ix, row in self.station_df.iterrows():
                if pd.notna(row[col]):
                    str_reformat = ' '.join(row[col].split())
                    str_list = str_reformat.split(' ')
                    self.station_df.at[ix, col] = '; '.join(str_list)
            # Replace None values and 'nan' strings with NaN
            self.station_df[col] = self.station_df[col].replace(to_replace=[None, 'nan', 'None'],
                                                                value=np.nan)

        # Geocode Status replacement
        self.station_df = self.geocode_status_formatting(dataframe=self.station_df,
                                                         df_col='geocode_status')

        # Facility Type replacement
        self.station_df = self.facility_type_formatting(dataframe=self.station_df,
                                                        df_col='facility_type')

        # Maximum Vehicle Class replacement
        self.station_df = self.maximum_vehicle_class_formatting(dataframe=self.station_df,
                                                                df_col='maximum_vehicle_class')

        # Convert DataFrame to GeoDataFrame
        self.station_df = gpd.GeoDataFrame(self.station_df,
                                           geometry=gpd.points_from_xy(x=self.station_df.longitude,
                                                                       y=self.station_df.latitude),
                                           crs=self.crs)

    @staticmethod
    def state_name_formatting(dataframe, df_col):
        value_replace = {
            'AK': 'Alaska',
            'AL': 'Alabama',
            'AR': 'Arkansas',
            # 'AS': 'American Samoa',
            'AZ': 'Arizona',
            'CA': 'California',
            'CO': 'Colorado',
            'CT': 'Connecticut',
            'DC': 'District of Columbia',
            'DE': 'Delaware',
            'FL': 'Florida',
            'GA': 'Georgia',
            # 'GU': 'Guam',
            'HI': 'Hawaii',
            'IA': 'Iowa',
            'ID': 'Idaho',
            'IL': 'Illinois',
            'IN': 'Indiana',
            'KS': 'Kansas',
            'KY': 'Kentucky',
            'LA': 'Louisiana',
            'MA': 'Massachusetts',
            'MD': 'Maryland',
            'ME': 'Maine',
            'MI': 'Michigan',
            'MN': 'Minnesota',
            'MO': 'Missouri',
            # 'MP': 'Northern Mariana Islands',
            'MS': 'Mississippi',
            'MT': 'Montana',
            # 'NA': 'National',
            'NC': 'North Carolina',
            'ND': 'North Dakota',
            'NE': 'Nebraska',
            'NH': 'New Hampshire',
            'NJ': 'New Jersey',
            'NM': 'New Mexico',
            'NV': 'Nevada',
            'NY': 'New York',
            'OH': 'Ohio',
            'OK': 'Oklahoma',
            'OR': 'Oregon',
            'PA': 'Pennsylvania',
            # 'PR': 'Puerto Rico',
            'RI': 'Rhode Island',
            'SC': 'South Carolina',
            'SD': 'South Dakota',
            'TN': 'Tennessee',
            'TX': 'Texas',
            'UT': 'Utah',
            'VA': 'Virginia',
            # 'VI': 'Virgin Islands',
            'VT': 'Vermont',
            'WA': 'Washington',
            'WI': 'Wisconsin',
            'WV': 'West Virginia',
            'WY': 'Wyoming'
        }

        # Replace values in dataframe
        dataframe[df_col] = dataframe[df_col].replace(value_replace)

        return dataframe

    @staticmethod
    def status_code_formatting(dataframe, df_col):
        value_replace = {
            'E': 'Available',
            'P': 'Planned',
            'T': 'Temporarily Unavailable'
        }

        # Replace values in dataframe
        dataframe[df_col] = dataframe[df_col].replace(value_replace)

        return dataframe

    @staticmethod
    def ev_networking_formatting(dataframe, df_col):
        value_replace = {
            'AddÉnergie Technologies': 'AddÉnergie',
            'AMPUP': 'AmpUp',
            'BCHYDRO': 'BC Hydro',
            'Blink Network': 'Blink',
            'CHARGELAB': 'ChargeLab',
            'ChargePoint Network': 'ChargePoint',
            'CHARGEUP': 'ChargeUp',
            'CIRCLE_K': 'CircleK Charge',
            'COUCHE_TARD': 'CircleK/Couche-Tard Recharge',
            'Circuit électrique': 'Circuit électrique',
            'eCharge Network': 'eCharge Network',
            'Electrify America': 'Electrify America',
            'Electrify Canada': 'Electrify Canada',
            'EVCS': 'EV Charging Solutions',
            'EV Connect': 'EV Connect',
            'EVGATEWAY': 'evGateway',
            'eVgo Network': 'EVgo',
            'EVRANGE': 'EV Range',
            'FLASH': 'FLASH',
            'FLO': 'FLO',
            'FPLEV': 'FPL EVolution',
            'FCN': 'Francis',
            'GRAVITI_ENERGY': 'Graviti Energy',
            'IVY': 'Ivy',
            'LIVINGSTON': 'Livingston Energy Group',
            'Non-Networked': 'Non-Networked',
            'NOODOE': 'Noodoe',
            'OpConnect': 'OpConnect',
            'PETROCAN': 'Petro-Canada',
            'POWERFLEX': 'PowerFlex',
            'RED_E': 'Red E Charging',
            'RIVIAN_ADVENTURE': 'Rivian Adventure Network',
            'RIVIAN_WAYPOINTS': 'Rivian Waypoints',
            'SemaCharge Network': 'SemaConnect',
            'SHELL_RECHARGE': 'Shell Recharge',
            'Sun Country Highway': 'Sun Country Highway',
            'SWTCH': 'Swtch Energy',
            'Tesla Destination': 'Tesla Destination',
            'Tesla': 'Tesla Supercharger',
            'UNIVERSAL': 'Universal EV Chargers',
            'Volta': 'Volta',
            'WAVE': 'WAVE',
            'Webasto': 'Webasto',
            'ZEFNET': 'ZEF Network',
        }

        # Replace values in dataframe
        dataframe[df_col] = dataframe[df_col].replace(value_replace)

        return dataframe

    @staticmethod
    def owner_type_formatting(dataframe, df_col):
        value_replace = {
            'FG': 'Federal Government Owned',
            'J': 'Jointly Owned',
            'LG': 'Local/Municipal Government Owned',
            'P': 'Privately Owned',
            'SG': 'State/Provincial Government Owned',
            'T': 'Utility Owned',
        }

        # Replace values in dataframe
        dataframe[df_col] = dataframe[df_col].replace(value_replace)

        return dataframe

    @staticmethod
    def geocode_status_formatting(dataframe, df_col):
        value_replace = {
            'GPS': 'GPS',
            '200-9': 'Point',
            '200-8': 'Address',
            '200-7': 'Intersection',
            '200-6': 'Street',
            '200-5': 'Neighborhood',
            '200-4': 'City/Town',
            '200-3': 'County',
            '200-2': 'State/Province',
            '200-1': 'Country',
            '200-0': 'Unknown',
        }

        # Replace values in dataframe
        dataframe[df_col] = dataframe[df_col].replace(value_replace)

        return dataframe

    @staticmethod
    def facility_type_formatting(dataframe, df_col):
        value_replace = {
            'AIRPORT': 'Airport',
            'ARENA': 'Arena',
            'AUTO_REPAIR': 'Auto Repair Shop',
            'BANK': 'Bank',
            'B_AND_B': 'B&B',
            'BREWERY_DISTILLERY_WINERY': 'Brewery/Distillery/Winery',
            'CAMPGROUND': 'Campground',
            'CAR_DEALER': 'Car Dealer',
            'CARWASH': 'Carwash',
            'COLLEGE_CAMPUS': 'College Campus',
            'CONVENIENCE_STORE': 'Convenience Store',
            'CONVENTION_CENTER': 'Convention Center',
            'COOP': 'Co-Op',
            'FACTORY': 'Factory',
            'FED_GOV': 'Federal Government',
            'FIRE_STATION': 'Fire Station',
            'FLEET_GARAGE': 'Fleet Garage',
            'FUEL_RESELLER': 'Fuel Reseller',
            'GROCERY': 'Grocery Store',
            'HARDWARE_STORE': 'Hardware Store',
            'HOSPITAL': 'Hospital',
            'HOTEL': 'Hotel',
            'INN': 'Inn',
            'LIBRARY': 'Library',
            'MIL_BASE': 'Military Base',
            'MOTOR_POOL': 'Motor Pool',
            'MULTI_UNIT_DWELLING': 'Multi-Family Housing',
            'MUNI_GOV': 'Municipal Government',
            'MUSEUM': 'Museum',
            'NATL_PARK': 'National Park',
            'OFFICE_BLDG': 'Office Building',
            'OTHER': 'Other',
            'OTHER_ENTERTAINMENT': 'Other Entertainment',
            'PARK': 'Park',
            'PARKING_GARAGE': 'Parking Garage',
            'PARKING_LOT': 'Parking Lot',
            'PAY_GARAGE': 'Pay-Parking Garage',
            'PAY_LOT': 'Pay-Parking Lot',
            'PHARMACY': 'Pharmacy',
            'PLACE_OF_WORSHIP': 'Place of Worship',
            'PRISON': 'Prison',
            'PUBLIC': 'Public',
            'REC_SPORTS_FACILITY': 'Recreational Sports Facility',
            'REFINERY': 'Refinery',
            'RENTAL_CAR_RETURN': 'Rental Car Return',
            'RESEARCH_FACILITY': 'Research Facility/Laboratory',
            'RESTAURANT': 'Restaurant',
            'REST_STOP': 'Rest Stop',
            'RETAIL': 'Retail',
            'RV_PARK': 'RV Park',
            'SCHOOL': 'School',
            'GAS_STATION': 'Service/Gas Station',
            'SHOPPING_CENTER': 'Shopping Center',
            'SHOPPING_MALL': 'Shopping Mall',
            'STADIUM': 'Stadium',
            'STANDALONE_STATION': 'Standalone Station',
            'STATE_GOV': 'State/Provincial Government',
            'STORAGE': 'Storage Facility',
            'STREET_PARKING': 'Street Parking',
            'TNC': 'Transportation Network Company',
            'TRAVEL_CENTER': 'Travel Center',
            'TRUCK_STOP': 'Truck Stop',
            'UTILITY': 'Utility',
            'WORKPLACE': 'Workplace',
        }

        # Replace values in dataframe
        dataframe[df_col] = dataframe[df_col].replace(value_replace)

        return dataframe

    @staticmethod
    def maximum_vehicle_class_formatting(dataframe, df_col):
        value_replace = {
            'LD': 'Passenger vehicles (class 1-2)',
            'MD': 'Medium-duty (class 3-5)',
            'HD': 'Heavy-duty (class 6-8)'
        }

        # Replace values in dataframe
        dataframe[df_col] = dataframe[df_col].replace(value_replace)

        return dataframe

