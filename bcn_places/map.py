import os
import time
import folium
import pandas as pd
from selenium import webdriver


class Converter:

    def __init__(self):
        self.csv = 'places_output.csv'

    def create_map(self):

        coordinates = [41.3874, 2.1686]

        es_map = folium.Map(location=coordinates, zoom_start=13)

        df = pd.read_csv(self.csv)

        lat = df['lat']
        lon = df['lon']

        for x, y in zip(lat, lon):
            folium.Marker(([x, y])).add_to(es_map)

        output = 'output.html'
        es_map.save(output)

        return output

    def save_png(self):

        url = 'file://{0}/{1}'.format(os.getcwd(), self.create_map())

        driver = webdriver.Chrome()
        driver.get(url)

        time.sleep(5)
        driver.save_screenshot("output.png")
        driver.quit()
