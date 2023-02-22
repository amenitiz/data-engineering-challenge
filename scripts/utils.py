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

def save_csv(df, filename, format="csv", mode="overwrite", header = True):
    df.repartition(1).write.option("header",header).format(format).mode(mode).save(filename)