#!/usr/bin/env python3

"""
Description:
Manages Environment with environment.yml file
"""

__version__ = '1.2.5'

import argparse
import sys
import os
import re
from functools import cmp_to_key
from pathlib import Path
from subprocess import check_output, run, CalledProcessError, PIPE
from typing import Union, Any, NoReturn
from string import ascii_lowercase, digits
try:
    from _version import __version__ as current_version
except ImportError:
    current_version = ''

CWD = os.path.dirname(os.path.realpath(__file__))


class EnvironmentSetup:
    """
    Commands for setting up conda environment
    """
    ENVIRONMENT_FILE = os.path.join(CWD, 'environment.yml')
    PERMISSION_ERRORS = ('[WinError 5] Access is denied', '[Errno 13] Permission denied',
                         'NotWritableError: The current user does not have write permissions to a required path')

    def __init__(self):
        self.environment_yml_filepath = self.get_environment_file()
        self.environment_name = self.get_environment_name(self.environment_yml_filepath)
        self.environment_folder = self.get_environment_folder(self.environment_name)
        self.current_requirements = {}
        self.project_requirements = {}
        self.conda_pth_filepath = ''
        self.current_packages = set()
        self.__changes = {}

    @staticmethod
    def find_environment_file() -> str:
        """
        Finds environment file in repository
        :return: path to environment file
        """
        for root, _, files in os.walk(CWD, topdown=True):
            for file in files:
                if str(file) == 'environment.yml':
                    return os.path.join(root, file)
        return ''

    def get_environment_file(self) -> str:
        """
        Finds environment file, checks default location first
        :return: Path to environment file
        """
        print("* Getting environment file ...")
        if os.path.isfile(self.ENVIRONMENT_FILE):
            environment = self.ENVIRONMENT_FILE
        else:
            environment = self.find_environment_file()
        if environment:
            print(f"\tFound environment file: {environment}")
        else:
            print("\tNo Environment File Found")
        return environment

    @staticmethod
    def check_encoding(environment_yml_filepath: str, encoding: str) -> bool:
        """
        Checks encoding of environment file
        :param environment_yml_filepath: path to environment file
        :param encoding: encoding to check
        :return: True if encoding is correct, else False
        """
        try:
            with open(environment_yml_filepath, encoding=encoding, errors='strict') as env:
                env.readlines()
        except UnicodeError:
            return False
        else:
            return True

    @staticmethod
    def convert_encoding(environment_yml_filepath, from_encoding: str, to_encoding: str) -> None:
        """
        Converts encoding of file
        :param environment_yml_filepath: path to environment file
        :param from_encoding: source encoding
        :param to_encoding: target encoding
        """
        filepath = Path(environment_yml_filepath)
        filepath.write_text(filepath.read_text(encoding=from_encoding), encoding=to_encoding)

    @classmethod
    def get_environment_name(cls, environment_yml_filepath: str) -> str:
        """
        Reads environment name from environment.yml
        :param environment_yml_filepath: path to environment.yml
        :return: environment name
        """
        if os.path.isfile(environment_yml_filepath):
            if cls.check_encoding(environment_yml_filepath, 'utf16'):
                print("* Getting environment name ...")
                with open(environment_yml_filepath, 'r', encoding='utf16') as yaml:
                    for line in yaml:
                        if line.startswith("name: "):
                            environment_name = line.split(":")[1].strip()
                            print(f"\tFound environment name: {environment_name}")
                            return environment_name
            elif cls.check_encoding(environment_yml_filepath, 'utf8'):
                print("Environment file is in UTF-8 encoding, converting to UTF-16LE")
                cls.convert_encoding(environment_yml_filepath, 'utf8', 'utf16')
                return cls.get_environment_name(environment_yml_filepath)
            else:
                print(f"Invalid encoding for: {environment_yml_filepath}")
                print("Convert file to UTF16LE")
        print("\tNo Environment Name Found")
        return ''

    @staticmethod
    def get_environment_folder(environment_name: str) -> str:
        """
        Gets environment path from env list
        :param environment_name: name of environment
        :return: environment path
        """
        def backup(err: Exception) -> str:
            environment_path = ''
            print(f'Error running subprocess: {err}\n using backup method')
            conda_prefix = os.getenv('CONDA_PREFIX', '')
            if conda_prefix.endswith(environment_subfolder):
                environment_path = conda_prefix
            elif conda_prefix:
                envs = os.path.dirname(conda_prefix)
                path = os.path.join(envs, environment_name)
                if path.endswith(environment_subfolder) and os.path.isdir(path):
                    environment_path = path
            return environment_path

        if os.path.isdir(environment_name):
            return environment_name
        if environment_name:
            environment_folder = ''
            environment_subfolder = os.path.join('envs', environment_name)
            print("* Getting environment folder ...")
            cmd = "conda env list"
            try:
                result = run(cmd, env=os.environ, stdout=PIPE, shell=True, check=True)
                formatted_results = result.stdout.decode('utf-8').splitlines(keepends=False)
                for line in formatted_results:
                    if line.endswith(environment_subfolder):
                        environment_folder = line.split(" ")[-1]
                        break
            except CalledProcessError as ex:
                environment_folder = backup(ex)

            finally:
                if environment_folder:
                    print(f"\tFound environment: {environment_name} at {environment_folder}")
                    return environment_folder
                print("\tNo environment path found")
                return environment_folder

    def run(self) -> None:
        """
        Runs steps in order to create, check, and update conda environment
        """
        if self.environment_yml_filepath and self.environment_name and \
                self.is_valid_environment_name(self.environment_name):
            self.fix_path_variable()
            self.check_version()
            self.check_for_similarly_named_environments(self.environment_name)
            self.check_for_old_environments(self.environment_name)
            # create environment if not exist
            if not self.environment_folder:
                self.create_environment(self.environment_yml_filepath)
            else:
                # update environment requirements
                self.__get_requirements()
                print(f"* Checking environment.yml against {self.environment_name} ...")
                if self.are_newer_requirements(self.project_requirements, self.current_requirements):
                    print("\tFound updates in environment.yml")
                    print(f"* Checking {self.environment_name} against environment.yml ...")
                    if self.are_newer_requirements(self.current_requirements, self.project_requirements):
                        print(f"\tUpdates found in both environment.yml and {self.environment_name}")
                    else:
                        print(f"\tNo updates found in {self.environment_name}")
                    self.__changes = self.get_changes(self.project_requirements, self.current_requirements)
                    self.print_changes('environment.yml', self.environment_name, self.__changes)
                    self.update_environment_from_environment_yml(self.environment_name, self.environment_yml_filepath,
                                                                 self.__changes)
                else:
                    print("\tNo updates found in environment.yml")
                    self.rebuild_from_scratch(self.environment_name, self.environment_yml_filepath)
                self.update_environment_all_packages(self.environment_name)
                self.__get_requirements()
                print(f"* Checking {self.environment_name} against environment.yml ...")
                if self.are_newer_requirements(self.current_requirements, self.project_requirements):
                    print(f"\tUpdates found in {self.environment_name}")
                    self.__changes = self.get_changes(self.current_requirements, self.project_requirements)
                    self.print_changes(self.environment_name, 'environment.yml', self.__changes)
                    self.update_environment_yml_from_environment(self.environment_yml_filepath, self.environment_name)
                else:
                    print(f"\tNo updates found in {self.environment_name}")

            # make sure environment path is created
            if not os.path.isdir(self.environment_folder):
                print("* Getting environment folder ...")
                self.environment_folder = self.get_environment_folder(self.environment_name)
            if self.environment_folder:
                # get package info
                self.conda_pth_filepath = self.get_conda_pth_filepath()
                self.current_packages = self.find_all_packages()
                # update package info
                if (not os.path.isfile(self.conda_pth_filepath)) and len(self.current_packages) > 0:
                    self.create_conda_pth_file()
                elif os.path.isfile(self.conda_pth_filepath):
                    conda_pth_packages = self.get_conda_pth_packages()
                    if conda_pth_packages != self.current_packages:
                        self.create_conda_pth_file()
        elif not self.environment_yml_filepath:
            environment = os.environ.get('CONDA_PREFIX', '')
            _, environment_name = os.path.split(environment)
            if environment_name and environment_name != 'base':
                environment_yml = os.path.join(CWD, 'environment.yml')
                self.create_new_yml(environment_yml, environment_name)
        self.clean_all()
        self.check_for_conda_updates()

    @classmethod
    def create_new_yml(cls, environment_yml: str, environment_name: str) -> None:
        """
        Creates a new environment yml.
        :param environment_yml: path to new environment yml
        :param environment_name: name of environment
        """
        while (res := input(f"Do you want to create a new environment.yml name from: {environment_name}"
                            f"? ([y]/n): ").lower()) not in {"y", "n"}:
            print()
            break
        if res == 'n':
            print()
            return
        cls.update_environment_yml_from_environment(environment_yml, environment_name, True)

    @staticmethod
    def is_valid_environment_name(environment_name: str) -> bool:
        """
        Checks whether environment name is valid
        :param environment_name: name of environment
        :return: True if name is valid else False
        """
        allowed_characters = ascii_lowercase + digits + '_'
        bad_characters = set()
        for character in environment_name.lower():
            if character not in allowed_characters:
                bad_characters.add(character)
        if bad_characters:
            print(f"[WARNING] Invalid environment name in yml: {environment_name}\n")
            recommended_name = environment_name
            for character in bad_characters:
                recommended_name = recommended_name.replace(character, '_')
            print(f"Recommended name: {recommended_name}")
            return False
        return True

    def check_version(self, skip_question: bool = False) -> None:
        """
        Checks if a _version.py file exists and if that version matches what is found in the environment.yml. If it
        doesn't match user is prompted to update the environment.yml to new version.
        :param skip_question: Skips question if True
        """
        if current_version:
            name, version = self.get_name_version_from_environment_name(self.environment_name)
            if version != current_version:
                new_name = f"{name}_{current_version.replace('.', '_')}"
                if not skip_question:
                    print(f"\n[WARNING] code version: {current_version} does not match environment.yml version: "
                          f"{version}")
                    while (res := input(f"Do you want to update environment.yml name from: {self.environment_name} to "
                                        f"{new_name}? ([y]/n): ").lower()) not in {"y", "n"}:
                        print()
                        break
                    if res == 'n':
                        print()
                        return
                    print()
                with open(self.environment_yml_filepath, 'r', encoding='utf-16') as yml_old:
                    with open(f"{self.environment_yml_filepath}.tmp", 'w', encoding='utf-16') as yml_new:
                        for line in yml_old:
                            line = line.replace(self.environment_name, new_name)
                            yml_new.write(line)
                os.remove(self.environment_yml_filepath)
                os.rename(f"{self.environment_yml_filepath}.tmp", self.environment_yml_filepath)
                self.environment_name = new_name
                self.environment_folder = self.get_environment_folder(self.environment_name)

    @staticmethod
    def get_base_path() -> str:
        """
        Gets base conda path
        :return: base conda path
        """
        cmd = "conda env list"
        result = run(cmd, env=os.environ, stdout=PIPE, shell=True, check=True)
        formatted_results = result.stdout.decode('utf-8').splitlines(keepends=False)
        for line in formatted_results:
            if line.startswith('base '):
                base_path = line.split(' ')[-1].strip()
                return base_path
        print("\tNo base environment found")
        return ''

    @classmethod
    def fix_path_variable(cls) -> None:
        """
        Adds base conda path, /Scripts, and Library/bin to system path variable if user didn't during installation
        """
        path = os.environ.get('PATH')
        paths = path.split(';')
        base_path = cls.get_base_path()
        if base_path:
            if base_path not in paths:
                path += f';{base_path}'
                base_path_scripts = os.path.join(base_path, 'Scripts')
                path += f';{base_path_scripts}'
                base_path_bin = os.path.join(base_path, 'Library', 'bin')
                path += f';{base_path_bin}'
                os.environ['PATH'] = path

    @staticmethod
    def check_for_conda_updates():
        """
        Prompts the user if they want to update base conda
        """
        while (res := input("\nDo you want to check for updates to base conda? (y/[n]): ").lower()) not in {"y", "n"}:
            print()
            return
        if res == 'n':
            print()
            return
        cmd = "conda update -n base -c defaults conda"
        run(cmd, check=True)

    @classmethod
    def remove_environment(cls, environment_name: str) -> None:
        """
        Removes conda environment
        :param environment_name: name of environment
        """
        if environment_name:
            print(f"Removing environment {environment_name}")
            cls.set_always_yes()
            if os.path.isdir(environment_name):
                param = 'p'
            else:
                param = 'n'
            cmd = f"conda remove -{param} {environment_name} --all"
            backup = cls.run_command_as_admin(cmd)
            cls.__wrap_for_permission_error(run, cmd, backup, env=os.environ, shell=True, check=True, stderr=PIPE)

    @classmethod
    def check_for_similarly_named_environments(cls, environment_name: str) -> None:
        """
        Checks for similarly named environments
        :param environment_name: name of environment
        """
        if environment_name:
            similar_names = []
            cmd = "conda env list"
            result = run(cmd, env=os.environ, stdout=PIPE, shell=True, check=True)
            formatted_results = result.stdout.decode('utf-8').splitlines(keepends=False)
            for line in formatted_results:
                line_lower = line.lower()
                environment_name_lower = environment_name.lower()
                if line_lower.endswith(f"envs{os.sep}{environment_name_lower}") and not \
                        line.endswith(f"envs{os.sep}{environment_name}"):
                    environment_path = line.split(" ")[-1]
                    similar_names.append(environment_path)
            if similar_names:
                print("[WARNING] environment(s) found with similar names:")
                for name in similar_names:
                    print(f'\t{name}')
                print("This will cause errors referencing packages. These environments should be removed.")
                while (res := input("\nDo you want to remove all of these environments?\n([y]/n): ").lower()) \
                        not in {"y", "n"}:
                    print()
                    break
                if res == 'n':
                    print()
                    return
                for name in similar_names:
                    cls.remove_environment(name)

    @classmethod
    def get_name_version_from_environment_name(cls, environment_name: str) -> (str, str):
        """
        Splits environment name into name and version
        :param environment_name: name of environment
        :return: name and version of environment
        """
        name = None
        version = None
        if environment_name:
            parts = environment_name.split("_")
            for index, part in enumerate(parts):
                if part.isdigit():
                    name = "_".join(parts[:index])
                    version_parts = parts[index:]
                    if len(version_parts) == 1:
                        version_parts.append('0')
                    version = ".".join(version_parts)
                    break
        return name, version

    @classmethod
    def check_for_old_environments(cls, environment_name: str) -> None:
        """
        Checks if there is more than one older environment
        :param environment_name: environment to check
        """
        if environment_name:
            other_versions = {}
            name, version = cls.get_name_version_from_environment_name(environment_name)
            if name and version:
                cmd = "conda env list"
                result = run(cmd, env=os.environ, stdout=PIPE, shell=True, check=True)
                formatted_results = result.stdout.decode('utf-8').splitlines(keepends=False)
                for line in formatted_results:
                    if line.startswith(name + '_') and not line.startswith(environment_name):
                        env = line.split(' ', 1)[0]
                        env_name, env_version = cls.get_name_version_from_environment_name(env)
                        if name == env_name and env_version:
                            env_path = line.split(' ')[-1]
                            other_versions[env_version] = env_path
            if other_versions:
                cls.check_old_version(version, other_versions)

    @classmethod
    def check_old_version(cls, version: str, other_versions: dict):
        """
        Checks if other versions are older than current version
        :param version: version to compare to
        :param other_versions: all other versions found
        """
        older_versions = []
        for other_version in other_versions:
            if cls.newer_version(version, other_version) == 1:
                older_versions.append(other_version)
        if older_versions:
            if len(older_versions) > 1:
                print("[WARNING] multiple older environments found:")
                older_versions = sorted(older_versions, key=cmp_to_key(cls.newer_version))
                print(f"Older versions: {older_versions}")
                while (res := input(f"\nDo you want to remove the {len(older_versions) - 1} oldest "
                                    f"environment(s)? (y/[n]): ").lower()) not in {"y", "n"}:
                    print()
                    return
                if res == 'n':
                    print()
                    return
                for old in older_versions[:-1]:
                    env_path = other_versions.get(old)
                    cls.remove_environment(env_path)

    @classmethod
    def create_environment(cls, environment_yml_filepath: str) -> NoReturn:
        """
        Creates a new conda environment from environment file
        :param environment_yml_filepath: path to environment.yml
        """
        if os.path.isfile(environment_yml_filepath):
            print(f"* Creating environment from file: {environment_yml_filepath} ...\n")
            cls.set_always_yes()
            cmd = f'conda env create --file "{environment_yml_filepath}"'
            try:
                run(cmd, env=os.environ, shell=True, check=True, capture_output=True)
            except CalledProcessError as ex:
                ex_returns = []
                for message in [ex.output, ex.stdout, ex.stderr]:
                    if isinstance(message, (bytes, bytearray)):
                        message = message.decode(sys.getfilesystemencoding())
                    ex_returns.append(message)

                print(f"ERROR OUTPUT: \n{ex_returns[0]}")
                for error in cls.PERMISSION_ERRORS:
                    if error in ex_returns[2]:
                        print("RESUME CREATION")
                        return cls.__resume_creation(environment_yml_filepath)
                if 'Pip subprocess error' in ex_returns[2]:
                    print("RESUME PIP INSTALL")
                    return cls.__resume_pip_install(environment_yml_filepath)
                raise

    @classmethod
    def __resume_creation(cls, environment_yml_filepath: str) -> None:
        """
        Resume creation after creation fail do to pip permissions
        :param environment_yml_filepath: path to environment file
        """
        print("* Error occurred during environment creation. Resuming creation ...\n")

        file_requirements = cls.get_file_requirements(environment_yml_filepath)
        environment_name = cls.get_environment_name(environment_yml_filepath)
        environment_folder = cls.get_environment_folder(environment_name)
        if not environment_folder:
            command = f'conda env create --file "{environment_yml_filepath}"'
            cmd = ''
            if str(os.environ.get('ComSpec')).endswith('cmd.exe'):
                cmd += 'powershell '
            program, arguments = command.split(' ', 1)
            cmd += f'"Start-Process -Filepath {program} -ArgumentList ' + f"'{arguments}' -Verb runas" + '"'
            check_output(cmd, shell=True, env=os.environ, text=True)
        else:
            local_requirements = cls.get_local_requirements(environment_name)
            changes = cls.get_changes(file_requirements, local_requirements)
            cls.print_changes('environment.yml', environment_name, changes)
            cls.update_environment_from_environment_yml(environment_name, environment_yml_filepath, changes, False)

    @classmethod
    def __resume_pip_install(cls, environment_yml_filepath: str) -> None:
        """
        Resume creation after creation fail do to pip permissions
        :param environment_yml_filepath: path to environment file
        """
        print("* Error occurred during environment creation. Resuming creation ...\n")

        file_requirements = cls.get_file_requirements(environment_yml_filepath)
        environment_name = cls.get_environment_name(environment_yml_filepath)
        local_requirements = cls.get_local_requirements(environment_name)
        changes = cls.get_changes(file_requirements, local_requirements)
        cls.print_changes('environment.yml', environment_name, changes)
        cls.update_environment_from_environment_yml(environment_name, environment_yml_filepath, changes, False, True)

    @staticmethod
    def print_changes(name_1: str, name_2: str, changes: dict) -> None:
        """
        Prints changes to console
        :param name_1: source 1
        :param name_2: source 2
        :param changes: changes between sources
        """
        print(f"\nChanges to turn {name_2} into {name_1}:")

        def print_adds_removes(title: str, adds_removes: Union[list, dict]) -> None:
            if title == 'channels':
                for channel in adds_removes:
                    print(f'\t\t\t{channel}')
            else:
                for package, version in adds_removes.items():
                    print(f'\t\t\t{package}=={version}')

        def print_upgrades_downgrades(upgrades_downgrades: dict) -> None:
            for package, info in upgrades_downgrades.items():
                version_1, version_2 = info
                print(f'\t\t\t{package}=={version_2} --> {version_1}')

        for category, results in changes.items():
            added = results.get('added')
            removed = results.get('removed')
            upgraded = results.get('upgraded')
            downgraded = results.get('downgraded')
            if added or removed or upgraded or downgraded:
                print(f"\t{category}:")
            if added:
                print('\t\tADD')
                print_adds_removes(category, added)
            if removed:
                print('\t\tREMOVE')
                print_adds_removes(category, removed)
            if upgraded:
                print('\t\tUPGRADE')
                print_upgrades_downgrades(upgraded)
            if downgraded:
                print('\t\tDOWNGRADE')
                print_upgrades_downgrades(downgraded)

    @staticmethod
    def clean_all() -> None:
        """
        Removes unused and cached packages
        """
        while (res := input("\nDo you want to remove unused packages and caches to free up disk space? (y/[n]): "
                            "").lower()) not in {"y", "n"}:
            print()
            return
        if res == 'n':
            print()
            return
        cmd = "conda clean --all -y"
        run(cmd, check=True)

    @classmethod
    def rebuild_from_scratch(cls, environment_name: str, environment_yml_filepath: str) -> None:
        """
        Remove and then create environment
        :param environment_name: name of environment
        :param environment_yml_filepath: path to environment yml
        """
        while (res := input(f"\nDo you want to completely rebuild {environment_name} from scratch? (y/[n]): "
                            "").lower()) not in {"y", "n"}:
            print()
            return
        if res == 'n':
            print()
            return
        cls.remove_environment(environment_name)
        cls.create_environment(environment_yml_filepath)

    @staticmethod
    def set_always_yes() -> None:
        """
        Sets always yes to prevent prompts
        """
        os.environ['CONDA_ALWAYS_YES'] = "false"

    def __get_requirements(self) -> None:
        """
        Gets current and project requirements
        """
        self.current_requirements = self.get_local_requirements(self.environment_name)
        print('\tDONE')
        self.project_requirements = self.get_file_requirements(self.environment_yml_filepath)
        print('\tDONE')

    @classmethod
    def get_local_requirements(cls, environment_name: str) -> dict:
        """
        Gets current requirements from environment
        :param environment_name: name of environment
        :return: dictionary of requirements
        """
        if environment_name:
            print(f"* Getting requirements for {environment_name} ...")
            cmd = f'conda env export --name {environment_name}'
            result = run(cmd, env=os.environ, stdout=PIPE, shell=True, check=True)
            formatted_results = result.stdout.decode('utf-8').splitlines(keepends=False)
            requirements = cls.parse_requirements(formatted_results)
            pip_git_packages = cls.get_local_pip_git_packages(environment_name)
            pip = dict(requirements.get('pip'))
            pip_git = requirements.get('pip_git')
            for package, link in pip_git_packages.items():
                if package in pip.keys():
                    version = pip.pop(package)
                    pip_git[package] = (version, link)
            requirements['pip'] = pip
            requirements['pip_git'] = pip_git
            return requirements
        return {}

    @staticmethod
    def parse_requirements(requirement_list: list) -> dict:
        """
        Parse requirements list into requirements dictionary
        :param requirement_list: list of requirements
        :return: dictionary of requirements
        """
        requirements = {'channels': [], 'dependencies': {}, 'pip': {}, 'pip_git': {}}
        active_section = ''
        for line in requirement_list:
            line = line.strip()
            if line.startswith('-'):
                line = line.replace('-', '', 1).strip()
            start = line.split(' ')[0]
            if start.endswith(':'):
                active_section = start[:-1]
                continue
            if active_section in requirements:
                if active_section == 'channels':
                    channels = requirements.get(active_section)
                    channels.append(line)
                    requirements[active_section] = channels
                elif active_section == 'dependencies':
                    parts = line.split('=')
                    package = parts[0]
                    version = parts[1]
                    dependencies = requirements[active_section]
                    dependencies[package] = version
                    requirements[active_section] = dependencies
                elif active_section == 'pip':
                    if 'git+https' in line:
                        link, version = line.split(' #==')
                        package = link.split('=')[-1]
                        package = str(package).replace('_', '-')
                        pip_git = requirements['pip_git']
                        pip_git[package] = (version, link)
                        requirements['pip_git'] = pip_git
                    elif '==' in line:
                        package, version = line.split('==')
                        pip = requirements[active_section]
                        pip[package] = version
                        requirements[active_section] = pip
                    else:
                        raise ValueError
        return requirements

    @classmethod
    def get_local_pip_git_packages(cls, environment_name: str) -> dict:
        """
        Gets the pip git packages
        :param environment_name: name of environment
        :return: local pip git packages
        """
        git_packages = {}
        cmd = 'pip freeze'
        results = cls.__activate_environment(environment_name, cmd)
        for result in results.splitlines(keepends=False):
            if ' @ ' in result:
                package, link = result.split(' @ ')
                if 'git+https' in link:
                    git_packages[package] = link
        return git_packages

    @staticmethod
    def run_command_as_admin(command: str, prefix: str = None) -> str:
        """
        Returns command with admin prompt
        :param command: original command
        :param prefix: prefix for command
        :return: command with admin access
        """
        if prefix:
            cmd = prefix
        else:
            cmd = ''
        if str(os.environ.get('ComSpec')).endswith('cmd.exe'):
            cmd += 'powershell '
        program, arguments = command.split(' ', 1)
        cmd += f'"Start-Process -Filepath {program} -ArgumentList ' + f"'{arguments}' -Verb runas" + '"'
        return cmd

    @classmethod
    def __wrap_for_permission_error(cls, function, cmd, backup, **kwargs) -> Any:
        """
        If function fails from permission error make admin
        :param function: function to call
        :param cmd: command to run
        :param backup: backup command to run
        :param kwargs: kwargs for command or backup
        :return: result of function
        """
        try:
            return function(cmd, **kwargs)
        except CalledProcessError as ex:
            if isinstance(ex.stderr, (bytes, bytearray)):
                error_message = ex.stderr.decode(sys.getfilesystemencoding())
            else:
                error_message = ex.stderr
            for err in cls.PERMISSION_ERRORS:
                if err in error_message:
                    print("[WARNING] Permission Error. Attempting to run command as admin in separate window."
                          "\n Wait until other window is complete. Click allow if prompted by windows security.")
                    return function(backup, **kwargs)
            print("Error message: ", error_message)
            raise ex

    def run_command(self, command: str):
        """
        Runs a command inside an active environment
        :param command: command to execute
        """
        self.__activate_environment(self.environment_name, command)

    @classmethod
    def __activate_environment(cls, environment: str, command: str) -> str:
        """
        Activates conda environment in local shell
        :param environment: path or name of environment
        :param command: command to run
        """
        if environment:
            cls.set_always_yes()
            cls.set_channel_priority()
            new_environ = cls.__get_new_environ(environment)
            backup = cls.run_command_as_admin(command)
            return cls.__wrap_for_permission_error(check_output, command, backup, shell=True, env=new_environ,
                                                   text=True, stderr=PIPE)
        return ''

    @classmethod
    def __get_new_environ(cls, environment: str) -> dict:
        """
        Gets new os environ file
        :param environment: name of environment
        :return: dictionary of environment variables
        """
        new_environ = {}
        if os.path.isdir(environment):
            new_folder, new_name = os.path.split(environment)
        else:
            path = cls.get_environment_folder(environment)
            new_folder, new_name = os.path.split(path)
        current_environment = os.environ.get('CONDA_PREFIX')
        current_folder, current_name = os.path.split(current_environment)
        for key, value in os.environ.items():
            if current_folder in value:
                value = value.replace(current_folder, new_folder)
            if current_name in value:
                value = value.replace(current_name, new_name)
            new_environ[key] = value
        return new_environ

    @staticmethod
    def set_channel_priority() -> None:
        """
        conda config --set channel_priority false
        """
        cmd = 'conda config --set channel_priority false'
        run(cmd, env=os.environ, shell=True, check=True)
        os.environ['CONDA_CHANNEL_PRIORITY'] = "false"

    @classmethod
    def get_file_requirements(cls, environment_yml_filepath: str) -> dict:
        """
        Gets project requirements from environment.yml
        :param environment_yml_filepath: path to environment file
        :return: dictionary of requirements
        """
        if os.path.isfile(environment_yml_filepath):
            print(f"* Getting requirements for {environment_yml_filepath} ...")
            with open(environment_yml_filepath, encoding='utf-16') as yaml:
                lines = yaml.read().splitlines(keepends=False)
            return cls.parse_requirements(lines)
        return {}

    @staticmethod
    def version_to_numbers(version: str) -> list:
        """
        Changes version to list of ints
        :param version: string version of package
        :return: list of version numbers
        """
        version_ints = []
        if '.' in version:
            version_numbers = version.split('.')
            for number in version_numbers:
                if number == 'git':
                    version_ints.append(number)
                elif any(character.isalpha() for character in number):
                    number = number.replace('post', '')
                    number = number.replace('dev', '')
                    parts = re.findall('\\d+|\\D+', number)
                    for part in parts:
                        if str(part).isdigit():
                            version_ints.append(int(part))
                        else:
                            # if multi letters use first
                            part = part[:1]
                            part = ord(part) - 96
                            version_ints.append(part)
                else:
                    version_ints.append(int(number))
        else:
            version_ints.append(version)
        return version_ints

    @classmethod
    def newer_version(cls, version_1: str, version_2: str) -> int:
        """
        Compares 2 versions together, if 1 is newer True else False
        :param version_1: version to see if newer
        :param version_2: version to compare
        :return: True if version 1 is newer
        """
        version_1_numbers = cls.version_to_numbers(version_1)
        version_2_numbers = cls.version_to_numbers(version_2)
        for count, number in enumerate(version_1_numbers):
            if count + 1 > len(version_2_numbers):
                return 1
            number_2 = version_2_numbers[count]
            if number == 'git' or number_2 == 'git':
                return 0
            if number > number_2:
                return 1
            if number < number_2:
                return -1
        return 0

    @classmethod
    def are_newer_requirements(cls, requirements_1: dict, requirements_2: dict) -> bool:
        """
        Compares 2 sets of requirements to see if 1 is newer than 2
        :param requirements_1: first set of requirements
        :param requirements_2: second set of requirements
        :return: True if requirements 1 is newer than requirements 2
        """
        for key, value in requirements_1.items():
            if key == 'channels':
                for channel in value:
                    if channel not in requirements_2.get('channels'):
                        return True
            elif key == 'pip_git':
                for package, info in value.items():
                    if package not in requirements_2.get(key):
                        return True
                    version = info[0]
                    info_2 = requirements_2.get(key).get(package)
                    version_2 = info_2[0]
                    if cls.newer_version(version, version_2) == 1:
                        return True
            else:
                for package, version in value.items():
                    if package not in requirements_2.get(key):
                        return True
                    version_2 = requirements_2.get(key).get(package)
                    if cls.newer_version(version, version_2) == 1:
                        return True
        return False

    @staticmethod
    def get_differences(requirements_1: dict, requirements_2: dict) -> dict:
        """
        Gets differences between 2 sets of requirements
        :param requirements_1: first set of requirements
        :param requirements_2: second set of requirements
        :return: differences
        """
        channels_1 = set(requirements_1.get('channels'))
        channels_2 = set(requirements_2.get('channels'))
        channel_differences_1 = channels_1 - channels_2
        channel_differences_2 = channels_2 - channels_1

        dependencies_1 = set(requirements_1.get('dependencies').items())
        dependencies_2 = set(requirements_2.get('dependencies').items())
        dependency_differences_1 = dependencies_1 - dependencies_2
        dependency_differences_2 = dependencies_2 - dependencies_1

        pip_1 = set(requirements_1.get('pip').items())
        pip_2 = set(requirements_2.get('pip').items())
        pip_differences_1 = pip_1 - pip_2
        pip_differences_2 = pip_2 - pip_1

        pip_git_1 = set(requirements_1.get('pip_git').items())
        pip_git_2 = set(requirements_2.get('pip_git').items())
        pip_git_differences_1 = pip_git_1 - pip_git_2
        pip_git_differences_2 = pip_git_2 - pip_git_1

        return {
            'channels': (channel_differences_1, channel_differences_2),
            'dependencies': (dependency_differences_1, dependency_differences_2),
            'pip': (pip_differences_1, pip_differences_2),
            'pip_git': (pip_git_differences_1, pip_git_differences_2)
        }

    @staticmethod
    def get_channel_changes(differences_1: set, differences_2: set) -> dict:
        """
        Gets changes between channels
        :param differences_1: channel differences 1
        :param differences_2: channel differences 2
        :return: channel changes
        """
        added = []
        removed = []
        if differences_1:
            for difference in differences_1:
                added.append(difference)
        if differences_2:
            for difference in differences_2:
                removed.append(difference)
        return {
            'added': added,
            'removed': removed
        }

    @classmethod
    def get_package_changes(cls, differences_1: set, differences_2: set) -> dict:
        """
        Gets changes between packages
        :param differences_1: package differences 1
        :param differences_2: package differences 2
        :return: package changes
        """
        added = {}
        removed = {}
        upgraded = {}
        downgraded = {}
        packages_1, versions_1 = cls.split_differences_into_packages_versions(differences_1)
        packages_2, versions_2 = cls.split_differences_into_packages_versions(differences_2)
        for count, package_1 in enumerate(packages_1):
            version_1 = versions_1[count]
            if package_1 not in packages_2:
                added[package_1] = version_1
            else:
                position = packages_2.index(package_1)
                version_2 = versions_2[position]
                if version_1 == version_2:
                    continue
                if cls.newer_version(version_1, version_2) == 1:
                    upgraded[package_1] = (version_1, version_2)
                else:
                    downgraded[package_1] = (version_1, version_2)
        for count, package_2 in enumerate(packages_2):
            version_2 = versions_2[count]
            if package_2 not in packages_1:
                removed[package_2] = version_2
        return {
            'added': added,
            'removed': removed,
            'upgraded': upgraded,
            'downgraded': downgraded
        }

    @staticmethod
    def split_differences_into_packages_versions(differences: set) -> (list, list):
        """
        Gets packages and versions from differences
        :param differences: differences
        :return: packages, versions
        """
        packages = []
        versions = []
        for package, version in sorted(differences):
            packages.append(package)
            versions.append(version)
        return packages, versions

    @classmethod
    def get_pip_git_changes(cls, differences_1: set, differences_2: set) -> dict:
        """
        Gets pip git changes
        :param differences_1: pip git differences 1
        :param differences_2: pip git differences 2
        :return: pip git changes
        """
        if differences_1:
            packages_1, info_1 = cls.split_differences_into_packages_versions(differences_1)
            versions_1, links_1 = zip(*info_1)
            pip_git_1 = set(zip(packages_1, versions_1))
            packages_links_1 = dict(zip(packages_1, links_1))
        else:
            pip_git_1 = differences_1
            packages_links_1 = None
        if differences_2:
            packages_2, info_2 = cls.split_differences_into_packages_versions(differences_2)
            versions_2, _ = zip(*info_2)
            pip_git_2 = set(zip(packages_2, versions_2))
        else:
            pip_git_2 = differences_2
        pip_git_changes = cls.get_package_changes(pip_git_1, pip_git_2)
        if packages_links_1:
            for section, values in pip_git_changes.items():
                new_values = {}
                if section == 'removed':
                    continue
                for package, versions in values.items():
                    link = packages_links_1.get(package)
                    new_values[link] = versions
                pip_git_changes[section] = new_values
        return pip_git_changes

    @classmethod
    def get_changes(cls, requirements_1: dict, requirements_2: dict) -> dict:
        """
        Gets changes from 2 requirements
        :param requirements_1: 1st set of requirements
        :param requirements_2: 2nd set of requirements
        :return: dictionary of changes
        """
        differences = cls.get_differences(requirements_1, requirements_2)

        channels_1, channels_2 = differences.get('channels')
        channel_changes = cls.get_channel_changes(channels_1, channels_2)

        dependencies_1, dependencies_2 = differences.get('dependencies')
        dependency_changes = cls.get_package_changes(dependencies_1, dependencies_2)

        pip_1, pip_2 = differences.get('pip')
        pip_changes = cls.get_package_changes(pip_1, pip_2)

        pip_git_1, pip_git_2 = differences.get('pip_git')
        pip_git_changes = cls.get_pip_git_changes(pip_git_1, pip_git_2)

        return {
            'channels': channel_changes,
            'dependencies': dependency_changes,
            'pip': pip_changes,
            'pip_git': pip_git_changes
        }

    @classmethod
    def update_environment_from_environment_yml(cls, environment_name: str, environment_yml_filepath: str,
                                                changes: dict = None, ask: bool = True, pip_only=False) -> None:
        """
        Updates current environment from environment.yml
        :param environment_name: name of environment
        :param environment_yml_filepath: path to environment file
        :param changes: changes between environment name and environment file
        :param ask: skip asking user
        :param pip_only: skip update command
        """
        if ask:
            while (res := input(f"\nDo you want to update environment: {environment_name} from: "
                                f"{environment_yml_filepath}? ([y]/n): ").lower()) not in {"y", "n"}:
                print()
                break
            if res == 'n':
                print()
                return

        if changes is None:
            local_requirements = cls.get_local_requirements(environment_name)
            file_requirements = cls.get_file_requirements(environment_yml_filepath)
            changes = cls.get_changes(file_requirements, local_requirements)
            cls.print_changes(environment_yml_filepath, environment_name, changes)

        # remove channels
        channels = changes.get('channels')
        if channels:
            remove_channels = channels.get('removed')
            if remove_channels:
                for channel in remove_channels:
                    cls.remove_channel(environment_name, channel)
        # pip
        pip_changes = changes.get('pip')
        cls.__process_pip_changes(environment_name, pip_changes)
        pip_git_changes = changes.get('pip_git')
        cls.__process_pip_git_changes(environment_name, pip_git_changes)
        # conda
        cls.set_always_yes()
        cls.set_channel_priority()
        dependencies = changes.get('dependencies')
        if not pip_only and dependencies:
            cmd = f'conda env update --name {environment_name} --file "{environment_yml_filepath}" --prune'
            backup = cls.run_command_as_admin(cmd)
            print(f"\n* Updating Environment: {environment_name} ...")
            try:
                cls.__wrap_for_permission_error(run, cmd, backup, env=os.environ, shell=True, check=True, stderr=PIPE)
            except CalledProcessError as ex:
                message = ex.stderr
                if isinstance(message, (bytes, bytearray)):
                    message = message.decode(sys.getfilesystemencoding())
                if 'Pip subprocess error' in message:
                    print("RESUME PIP INSTALL")
                    return cls.__resume_pip_install(environment_yml_filepath)

    @classmethod
    def remove_channel(cls, environment_name: str, channel: str) -> None:
        """
        Remove channel from environment
        :param environment_name: name of environment
        :param channel: channel to remove
        """
        cmd = f"conda config --remove {channel}"
        cls.__activate_environment(environment_name, cmd)

    @classmethod
    def __process_pip_changes(cls, environment_name: str, pip_changes: dict) -> None:
        """
        Processes pip changes
        :param environment_name: name of environment
        :param pip_changes: pip changes
        """
        added = pip_changes.get('added')
        removed = pip_changes.get('removed')
        upgraded = pip_changes.get('upgraded')
        downgraded = pip_changes.get('downgraded')

        if added:
            for package, version in added.items():
                cls.add_pip_package(environment_name, package, version)
        if removed:
            for package in removed.keys():
                cls.remove_pip_package(environment_name, package)
        if upgraded:
            for package, versions in upgraded.items():
                version_2 = versions[1]
                cls.upgrade_pip_package(environment_name, package, version_2)
        if downgraded:
            for package, versions in downgraded.items():
                version_2 = versions[1]
                cls.downgrade_pip_package(environment_name, package, version_2)

    @classmethod
    def __process_pip_git_changes(cls, environment_name: str, pip_git_changes: dict) -> None:
        """
        Process pip git changes
        :param environment_name: name of environment
        :param pip_git_changes: pip git changes
        """
        added = pip_git_changes.get('added')
        removed = pip_git_changes.get('removed')
        upgraded = pip_git_changes.get('upgraded')
        downgraded = pip_git_changes.get('downgraded')

        if added:
            for package in added.keys():
                cls.add_pip_package(environment_name, package)
        if removed:
            for package in removed.keys():
                cls.remove_pip_package(environment_name, package)
        if upgraded:
            for package in upgraded.keys():
                cls.upgrade_pip_package(environment_name, package)
        if downgraded:
            for package in downgraded.keys():
                cls.downgrade_pip_package(environment_name, package)

    @classmethod
    def add_pip_package(cls, environment_name: str, package: str, version: str = None) -> None:
        """
        Adds pip package to environment name
        :param environment_name: name of environment
        :param package: package to add
        :param version: version of package
        """
        cmd = f"pip install -q {package}"
        if version:
            cmd += f'=={version}'
        cls.__activate_environment(environment_name, cmd)

    @classmethod
    def remove_pip_package(cls, environment_name: str, package: str) -> None:
        """
        Remove a pip package
        :param environment_name: name of environment
        :param package: package to remove
        """
        cmd = f"pip uninstall -q --yes {package}"
        cls.__activate_environment(environment_name, cmd)

    @classmethod
    def upgrade_pip_package(cls, environment_name: str, package: str, version: str = None) -> None:
        """
        Upgrade a pip package
        :param environment_name: name of environment
        :param package: package to upgrade
        :param version: version of package
        """
        cmd = f"pip install -q {package}"
        if version:
            cmd += f'=={version} --ignore-installed'
        else:
            cmd += ' --upgrade'
        cls.__activate_environment(environment_name, cmd)

    @classmethod
    def downgrade_pip_package(cls, environment_name: str, package: str, version: str = None) -> None:
        """
        Downgrades a pip package
        :param environment_name: name of environment
        :param package: package to downgrade
        :param version: version of package
        """
        cmd = f"pip install -q {package}"
        if version:
            cmd += f'=={version}'
        cmd += ' --force-reinstall'
        cls.__activate_environment(environment_name, cmd)

    @classmethod
    def update_environment_all_packages(cls, environment_name: str, requirements: dict = None) -> None:
        """
        Updates all conda and pip packages
        :param environment_name: name of environment
        :param requirements: requirements of environment
        """
        while (res := input(f"\nDo you want to update your environment {environment_name} to latest available "
                            f"packages? \n[WARNING] May cause code instability. (y/[n]): ").lower()) not in {"y", "n"}:
            print()
            return
        if res == 'n':
            print()
            return
        print()

        cmd = f"conda update -n {environment_name} --all"
        backup = cls.run_command_as_admin(cmd)
        print(f"* Checking conda for updates to: {environment_name} ...")
        cls.set_always_yes()
        cls.set_channel_priority()
        cls.__wrap_for_permission_error(run, cmd, backup, shell=True, env=os.environ, check=True, stderr=PIPE)
        # pip
        if requirements is None:
            requirements = cls.get_local_requirements(environment_name)
        print(f"* Checking pip for updates to: {environment_name} ...")
        pip_requirements = requirements.get('pip')
        for package in pip_requirements.keys():
            cls.upgrade_pip_package(environment_name, package)
        pip_git_requirements = requirements.get('pip_git')
        for package, info in pip_git_requirements.items():
            link = info[1]
            cls.upgrade_pip_package(environment_name, link)

    @classmethod
    def update_environment_yml_from_environment(cls, environment_yml_filepath: str, environment_name: str,
                                                skip_question: bool = False, no_builds: bool = False) -> None:
        """
        Update environment file from current environment
        :param environment_yml_filepath: path to environment file
        :param environment_name: name fo environment
        :param skip_question: skips question
        :param no_builds: export env with no builds
        """
        if not skip_question:
            while (res := input(f"\nDo you want to update environment.yml from: {environment_name}? "
                                f"(y/[n]): ").lower()) not in {"y", "n"}:
                print()
                return
            if res == 'n':
                print()
                return

        cmd = 'conda env export '
        if no_builds:
            cmd += '--no-builds '
        cmd += f'-n {environment_name}'
        print(f'Updating Environment File: {environment_yml_filepath}')
        result = run(cmd, env=os.environ, stdout=PIPE, shell=True, check=True)
        formatted_results = result.stdout.decode('utf-8').splitlines(keepends=False)
        pip_git_packages = cls.get_local_pip_git_packages(environment_name)
        with open(environment_yml_filepath, 'w', encoding='utf-16') as env_file:
            for result in formatted_results[:-1]:
                if pip_git_packages:
                    for key, value in pip_git_packages.items():
                        if f' - {key}==' in result:
                            version = result.split('==')[1]
                            url = value.split('@')[0]
                            sub_dir = value.split('#')[1]
                            value = f"{url}#{sub_dir}"
                            # key is 8 spaces from start of line
                            result = f"{result[:8]}{value} #=={version}"
                env_file.write(f"{result}\n")

    @staticmethod
    def find_all_packages() -> set:
        """
        Finds all packages in the repository
        :return: packages
        """
        packages = set()
        for root, _, files in os.walk(CWD, topdown=True):
            for file in files:
                if str(file).endswith("__init__.py"):
                    if 'tests' not in root:
                        packages.add(os.path.dirname(root))
        return packages

    def get_conda_pth_filepath(self) -> str:
        """
        Gets conda.pth filepath
        """
        return os.path.join(self.environment_folder, 'Lib', 'site-packages', 'conda.pth')

    def create_conda_pth_file(self) -> None:
        """
        Creates conda.pth file
        """
        print(f"* Creating conda.pth file: {self.conda_pth_filepath}")
        with open(self.conda_pth_filepath, mode='w', encoding='utf8') as conda_pth:
            for path in self.current_packages:
                conda_pth.write(f'{path}\n')

    def get_conda_pth_packages(self) -> set:
        """
        Gets all conda.pth packages
        """
        with open(self.conda_pth_filepath, 'r', encoding='utf8') as conda_pth:
            packages = conda_pth.read().splitlines(keepends=False)
        return set(packages)

    def github_read(self) -> NoReturn:
        """
        Sets environment name and environment folder for github runner
        """
        self.check_version(skip_question=True)
        base_path = self.get_base_path()
        environment_folder = os.path.join(base_path, 'envs', self.environment_name)
        with open(os.getenv('GITHUB_OUTPUT'), 'a+', encoding='utf8') as env:
            name, _ = self.get_name_version_from_environment_name(self.environment_name)
            env.write(f'environment_base_name={name}\n')
            env.write(f'environment_name={self.environment_name}\n')
            env.write(f'environment_folder={environment_folder}\n')

    def github_update(self) -> NoReturn:
        """
        Updates the empty environment on GitHub runner
        """
        # conda update env
        self.set_always_yes()
        self.set_channel_priority()
        cmd = f'conda env update --name {self.environment_name} --file "{self.environment_yml_filepath}" --prune'
        backup = self.run_command_as_admin(cmd)
        print(f"\n* Updating Environment: {self.environment_name} ...")
        try:
            self.__wrap_for_permission_error(run, cmd, backup, env=os.environ, shell=True, check=True, stderr=PIPE)
        except CalledProcessError as ex:
            message = ex.stderr
            if isinstance(message, (bytes, bytearray)):
                message = message.decode(sys.getfilesystemencoding())
            if 'Pip subprocess error' in message:
                print("RESUME PIP INSTALL")
                return self.__resume_pip_install(self.environment_yml_filepath)

        # get env folder if not set
        if not os.path.isdir(self.environment_folder):
            print("* Getting environment folder ...")
            self.environment_folder = self.get_environment_folder(self.environment_name)

        # create conda.pth file
        if self.environment_folder:
            # get package info
            self.conda_pth_filepath = self.get_conda_pth_filepath()
            self.current_packages = self.find_all_packages()
            # update package info
            if (not os.path.isfile(self.conda_pth_filepath)) and len(self.current_packages) > 0:
                self.create_conda_pth_file()
            elif os.path.isfile(self.conda_pth_filepath):
                conda_pth_packages = self.get_conda_pth_packages()
                if conda_pth_packages != self.current_packages:
                    self.create_conda_pth_file()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--read', help="Reads the environment file, sets environment name and folder for github.",
                        action="store_true")
    parser.add_argument('-u', '--update', help="Updates environment from environment yml", action="store_true")
    args_ = parser.parse_args()
    env_setup = EnvironmentSetup()
    if args_.read:
        env_setup.github_read()
    elif args_.update:
        env_setup.github_update()
    else:
        env_setup.run()
