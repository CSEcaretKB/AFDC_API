

class DatabaseStructure:
    def __init__(self):
        self.db_structure = {}
        self.read_only_tables = ['station', 'evse']
        self.extract_db_structure()

    def extract_db_structure(self):
        self.db_structure = {
            'station': self.station(),
            'evse': self.evse()
        }

    @staticmethod
    def station():
        return """
        CREATE TABLE station (
            id VARCHAR(255) NOT NULL,
            station_name VARCHAR(255),
            street_address VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(255),
            zip  VARCHAR(50),
            latitude NUMERIC,
            longitude NUMERIC,
            geocode_status VARCHAR(255),
            ev_network VARCHAR(255),
            ev_connector_types VARCHAR(255),
            ev_level1_evse_num INTEGER, 
            ev_level2_evse_num INTEGER, 
            ev_dc_fast_num INTEGER,
            facility_type VARCHAR(255),
            maximum_vehicle_class VARCHAR(255),
            owner_type_code VARCHAR(255),
            access_code VARCHAR(255),
            status_code VARCHAR(255),
            ev_renewable_source VARCHAR(255),
            ev_pricing VARCHAR(255),
            open_date DATE,
            expected_date DATE,
            date_last_confirmed DATE,
            updated_at TIMESTAMPTZ,
            geometry GEOMETRY,
            PRIMARY KEY (id)
        )
        """

    @staticmethod
    def evse():
        return """
        CREATE TABLE evse (
            id VARCHAR(255) NOT NULL,
            station_id VARCHAR(255),
            evse_id VARCHAR(255),
            FOREIGN KEY (id)
                REFERENCES station (id)
        )
        """