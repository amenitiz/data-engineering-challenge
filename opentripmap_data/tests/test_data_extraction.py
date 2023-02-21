import sys

import pandas as pd

sys.path.append("..")

from opentripmap_data.data_extraction import (
    parse_opentripmap_location_entry,
    get_xids_from_locations_dataframe,
    parse_location_detail_address,
)


# Test parse_opentripmap_entry
def test_parse_opentripmap_entry_fully_populated():
    entry = {
        "xid": "N3355811093",
        "name": "Hotel Centre",
        "rate": 1,
        "osm": "node/3355811093",
        "wikidata": "Q11682616",
        "kinds": "accomodations,other_hotels",
        "point": {"lon": 2.0446624755859375, "lat": 41.38056945800781},
    }
    expected_result = [
        {
            "xid": "N3355811093",
            "name": "Hotel Centre",
            "rate": 1,
            "osm": "node/3355811093",
            "wikidata": "Q11682616",
            "kinds": "accomodations,other_hotels",
            "longitude": 2.0446624755859375,
            "latitude": 41.38056945800781,
        }
    ]
    assert parse_opentripmap_location_entry(entry) == expected_result


def test_parse_opentripmap_entry_no_wikidata():
    entry = {
        "xid": "N3355811093",
        "name": "Hotel Centre",
        "rate": 1,
        "osm": "node/3355811093",
        "kinds": "accomodations,other_hotels",
        "point": {"lon": 2.0446624755859375, "lat": 41.38056945800781},
    }
    expected_result = [
        {
            "xid": "N3355811093",
            "name": "Hotel Centre",
            "rate": 1,
            "osm": "node/3355811093",
            "wikidata": None,
            "kinds": "accomodations,other_hotels",
            "longitude": 2.0446624755859375,
            "latitude": 41.38056945800781,
        }
    ]
    assert parse_opentripmap_location_entry(entry) == expected_result


def test_parse_opentripmap_entry_no_osm():
    entry = {
        "xid": "N3355811093",
        "name": "Hotel Centre",
        "rate": 1,
        "wikidata": "Q11682616",
        "kinds": "accomodations,other_hotels",
        "point": {"lon": 2.0446624755859375, "lat": 41.38056945800781},
    }
    expected_result = [
        {
            "xid": "N3355811093",
            "name": "Hotel Centre",
            "rate": 1,
            "osm": None,
            "wikidata": "Q11682616",
            "kinds": "accomodations,other_hotels",
            "longitude": 2.0446624755859375,
            "latitude": 41.38056945800781,
        }
    ]
    assert parse_opentripmap_location_entry(entry) == expected_result


def test_parse_opentripmap_entry_no_osm_nor_wikidata():
    entry = {
        "xid": "N3355811093",
        "name": "Hotel Centre",
        "rate": 1,
        "kinds": "accomodations,other_hotels",
        "point": {"lon": 2.0446624755859375, "lat": 41.38056945800781},
    }
    expected_result = [
        {
            "xid": "N3355811093",
            "name": "Hotel Centre",
            "rate": 1,
            "osm": None,
            "wikidata": None,
            "kinds": "accomodations,other_hotels",
            "longitude": 2.0446624755859375,
            "latitude": 41.38056945800781,
        }
    ]
    assert parse_opentripmap_location_entry(entry) == expected_result


# Test get_xids_from_locations_dataframe
def test_get_xids_from_locations_dataframe():
    input_df = pd.DataFrame(
        {
            "xid": ["N3355811093", "N3355811123", "N335581123123"],
            "kinds": ["accomodations,other_hotels", "accomodations", "beach,sun,sea"],
        }
    )
    xids_list = ["N3355811093", "N3355811123", "N335581123123"]

    assert get_xids_from_locations_dataframe(input_df) == xids_list


# Test parse_location_detail_address
def test_parse_location_detail_address():
    full_address = {
        "city": "l'Hospitalet",
        "road": "Carrer de Jaume Ventura i Tort",
        "house": "Hotel Hesperia Barcelona Tower",
        "state": "CAT",
        "county": "BCN",
        "suburb": "Bellvitge",
        "country": "España",
        "postcode": "08907",
        "country_code": "es",
        "house_number": "144",
        "city_district": "Districte VI",
    }
    expected_result = (
        "Carrer de Jaume Ventura i Tort, 144. 08907 Bellvitge(BCN). España"
    )
    assert parse_location_detail_address(full_address) == expected_result
