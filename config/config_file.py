import os
import configparser

CONFIG_FILE = os.path.join('config.ini')

def load_config():
    """
    Load the configuration file
    """
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config

def save_config(config):
    """
    Save the configuration file
    """
    with open(CONFIG_FILE, 'w') as configfile:
        config.write(configfile)