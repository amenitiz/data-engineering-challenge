import os
import json
import plotly.express as px
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F

def check_path(filename):
    """
    Checks if filename path is usable.
    
    Parameters
    ----------
    filename: str
        Filename path.

    Returns
    -------
    filename: str
        Filename path.

    """
    parent_dir = os.path.dirname(filename)
    if parent_dir and not os.path.exists(parent_dir):
        try:
            os.makedirs(parent_dir)
        except:
            raise OSError('Cannot save file: %s' % filename)
    return filename

def load_json(fname):
    """
    Load json file.
    
    Parameters
    ----------
    fname: str
        Filename path.

    Returns
    -------
    out: dict
        Dictionary with json file data.

    """
    with open(fname) as f:
        out = json.load(f)
    return out

def save_csv(df, filename, format="csv", mode="overwrite", header = True):
    """
    Save DataFrame in csv format.
    
    Parameters
    ----------
    filename: str
        Filename path.
    format: str
        File format (csv, json, ...).
    mode: str
        Use "overwrite" to overwrite files.
    header: bool
        True to save header in the file.

    Returns
    -------
    None

    """
    df.repartition(1).write.option("header",header).format(format).mode(mode).save(filename)

def generate_open_street_map(df, filename):
    """
    Plot Accomodations in Open Trip Map using plotly
    
    Parameters
    ----------
    df: DataFrame
        PySpark DataFrame with Accomodation data.
    filename: str
        Filename path.

    Returns
    -------
    None

    """
    df = df.withColumn("lon", F.col("lon").astype(FloatType()))
    df = df.withColumn("lat", F.col("lat").astype(FloatType()))

    fig = px.scatter_mapbox(df.toPandas(), 
                            lat='lat',
                            lon='lon',
                            color='name',
                            zoom=13)

    fig.update_layout(mapbox_style='open-street-map')

    fig.show()

    fig.write_image(filename)
