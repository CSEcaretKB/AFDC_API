import os
from environment_setup import EnvironmentSetup, CalledProcessError
from src.tools.sql import SQL
from src.tools.api import API
from src.tools.data_extraction import DataExtractor


class Manager:
    def __init__(self):
        self.proj_path_fix()
        self.sql = SQL()
        self.api = API()
        self.extractor = DataExtractor(json_dict=self.api.json_dict)
        self.insert_station_data_into_db()
        self.insert_evse_data_into_db()

    def proj_path_fix(self):
        if os.environ.get('HOMEPATH') == r'\Users\colin.evans':
            self.proj_path_fix_simple()
        else:
            self.proj_path_fix_complex()

    @staticmethod
    def proj_path_fix_simple():
        """
        Checks proj lib/data variable for database path (bug fix)
        """
        proj = os.getenv('PROJ_DATA', os.getenv('PROJ_LIB'))
        proj_folder = os.path.join('Library', 'share', 'proj')
        if not proj.endswith(proj_folder):
            try:
                env_path = ''
                env_yml = EnvironmentSetup.find_environment_file()
                if env_yml:
                    env_name = EnvironmentSetup.get_environment_name(env_yml)
                    if env_name:
                        env_path = EnvironmentSetup.get_environment_folder(env_name)
            except CalledProcessError:
                pass
            else:
                if env_path:
                    proj = os.path.join(env_path, proj_folder)
                    if os.getenv('PROJ_DATA'):
                        os.environ['PROJ_DATA'] = proj
                    else:
                        os.environ['PROJ_LIB'] = proj
            try:
                from pyproj import datadir
                datadir.set_data_dir(os.getenv('PROJ_DATA', os.getenv('PROJ_LIB')))
            except ImportError:
                pass

    @staticmethod
    def proj_path_fix_complex(default_path: str = None, force_set: bool = False):
        """
        Fixes proj path errors for variable of database path (bug fix)
        :param default_path: set to this path and not use environment yml
        :param force_set: if True force sets pyproj to use path, False recommends path through env variables
        """
        env_proj_subfolder = os.path.join('Library', 'share', 'proj')

        def get_env_proj_path() -> str:
            current_env_default = os.getenv('PROJ_DATA', os.getenv('PROJ_LIB', ''))
            print(f"Current env PROJ path: {current_env_default}")

            env_path = ''
            env_proj_path = ''
            try:
                from environment_setup import EnvironmentSetup, CalledProcessError
            except ImportError:
                print("No environment_setup.py script found, not able to determine env path.")
            else:
                try:
                    env_yml = EnvironmentSetup.find_environment_file()
                    if env_yml:
                        env_name = EnvironmentSetup.get_environment_name(env_yml)
                        if env_name:
                            env_path = EnvironmentSetup.get_environment_folder(env_name)
                except CalledProcessError as ex:
                    print(f"Error reading environment.yml: {ex}")
                else:
                    if env_path:
                        env_proj_path = os.path.join(env_path, env_proj_subfolder)
            finally:
                if env_proj_path:
                    print(f"New env PROJ path: {env_proj_path}")
                return env_proj_path

        try:
            from pyproj.datadir import set_data_dir, get_data_dir
        except ImportError:
            print("Pyproj not installed.")
        else:
            current_proj_path = get_data_dir()
            print(f"Current PROJ Path: {current_proj_path}")
            if default_path:
                print(f"Using default path: {default_path}")
                new_path = default_path
            elif not current_proj_path.endswith(env_proj_subfolder):
                print("Getting env PROJ path...")
                new_path = get_env_proj_path()
                if not new_path:
                    print(f"Not able to get new path, current path not changed: {current_proj_path}")
                    return
            else:
                new_path = current_proj_path
            if new_path == current_proj_path:
                print("Already using correct PROJ Path")
                return
            else:
                if force_set:
                    print(f"Force setting PROJ path to: {new_path}")
                    set_data_dir(new_path)
                else:
                    print(f"Setting env PROJ path to: {new_path}")
                    if os.getenv('PROJ_DATA'):
                        os.environ['PROJ_DATA'] = new_path
                    else:
                        os.environ['PROJ_LIB'] = new_path

    def insert_station_data_into_db(self):
        failures = self.sql.df_insert_to_database(dataframe=self.extractor.station_df,
                                                  table_name=f'station')

    def insert_evse_data_into_db(self):
        failures = self.sql.df_insert_to_database(dataframe=self.extractor.evse_df,
                                                  table_name=f'evse')
