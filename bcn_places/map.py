import os
import time
import folium
import pandas as pd
from selenium import webdriver
from global_ import coordinates


class Converter:

    def __init__(self):
        self.df = pd.read_csv('places_output.csv')

    def create_map(self):
        es_map = folium.Map(location=coordinates, zoom_start=13)
        lat = self.df['lat']
        lon = self.df['lon']

        for x, y in zip(lat, lon):
            folium.Marker(([x, y])).add_to(es_map)

        return 'output.html'

    def save_png(self):
        url = 'file://{0}/{1}'.format(os.getcwd(), self.create_map())
        driver = webdriver.Chrome()
        driver.get(url)
        time.sleep(5)
        driver.save_screenshot("output.png")
        driver.quit()


