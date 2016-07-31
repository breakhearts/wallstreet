from __future__ import absolute_import
import json
import os

config_file_path = os.path.join(os.path.dirname(__file__) + "/../config.json")
with open(config_file_path, "rb") as f:
    config = json.loads(f.read().decode("utf-8"))


def get(section, key):
    return config[section][key]


def set_config(section, key, value):
    config[section][key] = value


def get_int(section, key):
    return int(get(section, key))


def get_float(section, key):
    return float(get(section, key))