# -*- coding: UTF-8 -*-
# HiroshimaEastAreaBusImport
# Copyright (C) 2023 Code for Hiroshima, Masataka Shinke
# This file is covered by the GNU General Public License v3.0
# See the file COPYING for more details.

import pandas as pd
import zipfile
import datetime
import urllib.request
import io
import json
import os


class gtfs2geojson:
    _agency_names_translations = None
    agency_name_id = None
    agency_name = None
    agency_name_en = None
    feed_version = None
    group = None

    def __init__(self, agency_filename="agency_table.csv") -> None:
        self._agency_names_translations = self._agency_table(
            agency_filename)

        for _, row in self._agency_names_translations.iterrows():
            with (
                urllib.request.urlopen(row.url) as res,
                io.BytesIO(res.read()) as bytes_io,
                zipfile.ZipFile(bytes_io) as zip,
            ):
                self.pos = 0
                self.zip_f = zip
                self.agency_name_id = self._agency()
                self.agency_name = self._agecy_name()
                self.agency_name_en = self._agecy_name_en()
                self.feed_version = self._feed_info()
                self.group = self._group()
                ret = self._load()
                os.makedirs("data", exist_ok=True)
                with open(os.path.join("data", self.agency_name_en.replace(" ", "_")+".geojson"), 'w') as f:
                    json.dump(ret, f, ensure_ascii=False, indent=4,
                              sort_keys=True, separators=(',', ': '))
                print(f'{row.url} -> {self.agency_name_en.replace(" ","_")}.geojson')

    def _load(self) -> str:
        j = []
        for i1 in self.group:
            j.append(i1)
        result = {"type": "FeatureCollection",
                  "features": j}
        return result

    def _agency_table(self, agency_filename) -> pd.DataFrame:
        return pd.read_csv(agency_filename, usecols=['operator_id', 'operator', 'operator:en', 'url'])

    def _agency(self) -> str:
        return pd.read_csv(
            self.zip_f.open("agency.txt"), usecols=['agency_name']).agency_name[0]

    def _feed_info(self) -> str:
        try:
            feed_info = str(pd.read_csv(
                self.zip_f.open("feed_info.txt"),
                usecols=['feed_version']).feed_version[0]).split("_")[0]
        except:
            feed_info = f"{datetime.date.today().year}(no date info.)"
        return feed_info

    def _translations(self) -> pd.DataFrame:
        return pd.read_csv(
            self.zip_f.open("translations.txt"),
            usecols=["trans_id", "lang", "translation"])

    def _stops(self) -> pd.DataFrame:
        return pd.read_csv(
            self.zip_f.open("stops.txt"),
            usecols=["stop_id", "stop_name", "stop_lat", "stop_lon"]
        )

    def _merge_stops(self) -> pd.DataFrame:
        return pd.merge(
            left=self._stops(),
            right=self._translations(),
            left_on="stop_name", right_on="trans_id")

    def _agecy_name(self) -> str:
        return self._agency_names_translations.loc[
            self._agency_names_translations['operator_id'] == self._agency(
            ), 'operator'].values[0]

    def _agecy_name_en(self) -> str:
        return self._agency_names_translations.loc[
            self._agency_names_translations['operator_id'] == self._agency(
            ), 'operator:en'].values[0]

    def _group(self) -> pd.DataFrame:
        group = self._merge_stops().groupby(
            ['stop_id', 'stop_name', 'stop_lat', 'stop_lon']
        ).apply(lambda d: self._join(d, d.lang, d.translation))
        return group

    def _join(self, d, lang, translation) -> dict:

        properties = {"agency_name": self.agency_name, "agency_name:en": self.agency_name_en,
                      "bus": "yes", "highway": "bus_stop", "public_transport": "platform"}

        k = 0
        lat = None
        lon = None
        for i in [i for i, j in d.items()]:
            if i in ['stop_id', 'stop_name', 'platform_code']:
                if i == 'stop_id':
                    properties[f"gtfs:{i}"] = str(d.name[k]).replace("_", " ")
                elif i == 'stop_name':
                    properties[f"name"] = d.name[k]                    
                else:
                    properties[i] = d.name[k]
            elif i in ['stop_lat']:
                lat = d.name[k]
            elif i in ['stop_lon']:
                lon = d.name[k]
            k += 1
        for i, j in lang.items():
            properties[f"name:{j}"] = translation[i]

        geometry = {"type": "Feature",
                    "geometry":
                    {
                        "type": "Point",
                        "coordinates": [lon, lat]
                    }, "properties": properties}

        return geometry


if __name__ == '__main__':
    gtfs2geojson()
