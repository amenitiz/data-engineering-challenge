import os
import json

def check_path(filename):
    parent_dir = os.path.dirname(filename)
    if parent_dir and not os.path.exists(parent_dir):
        try:
            os.makedirs(parent_dir)
        except:
            raise OSError('Cannot save file: %s' % filename)
    return filename

def load_json(fname):
    with open(fname) as f:
        out = json.load(f)
    return out

