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
except:
    pass


def get(section, key):
    try:
        return local_config[section][key]
    except:
        return config[section][key]


def get_path(section, key):
    t = get(section, key)
    if t.startswith("/"):
        return t
    else:
        return os.path.join(os.path.join(os.path.dirname(__file__), ".."), t)


def set_config(section, key, value):
    try:
        local_config[section][key] = value
    except:
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