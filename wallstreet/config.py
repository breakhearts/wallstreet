from __future__ import absolute_import
import json
import os

config_file_path = os.path.join(os.path.dirname(__file__) + "/../config.json")
config_local_file_path = os.path.join(os.path.dirname(__file__) + "/../config_local.json")
with open(config_file_path, "rb") as f:
    config = json.loads(f.read().decode("utf-8"))
try:
    with open(config_local_file_path, "rb") as f:
        local_config = json.loads(f.read().decode("utf-8"))
        config.update(local_config)
except:
    pass


def get(section, key):
    try:
        return local_config[section][key]
    except:
        return config[section][key]


def set_config(section, key, value):
    config[section][key] = value


def get_int(section, key):
    return int(get(section, key))


def get_float(section, key):
    return float(get(section, key))


def get_test(section, key):
    try:
        return get(section, key + "_test")
    except:
        return get(section, key)


def get_test_int(section, key):
    return int(get_test(section, key))


def get_test_float(section, key):
    return float(get_test(section, key))