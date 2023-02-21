# Amenitiz © Data Engineer challenge 1
- Name: Aleix Paradell
- Position: Data Engineer
- Date: February 2023
- [Challenge Instructions](challenge_instructions.md)


## Initial Set Up
1. Clone this repo `git clone git@github.com:paradell/taxdown-coding-challenge.git`
2. Install the latest **Python 3.8.X** version. To easily switch between Python versions, it is recommended to use [pyenv](https://github.com/pyenv/pyenv) or [virtualenv](https://virtualenv.pypa.io/en/latest/).
    - `pyenv install 3.8.X` (use the appropriate version)
    - `pyenv local 3.8.X`
    - Check Python version with `python --version`.
3. Install developer libraries
```bash
python -m pip install --upgrade pip
python -m pip install -r dev-requirements.txt
```
4. Install project libraries
```bash
python -m pip install -r requirements.txt
```
5. Install pre-commit configurations
```bash
pre-commit install
```

## Extract data from Open Trip Map
1. Have an Open Trip Map account.
2. Store the Open Trip Map API KEY as a Environment Variable
```bash
export OPENTRIPMAP_APIKEY={your_api_key}`
```
3. Run the data extraction code.
```bash
python opentripmap_data/data_extraction.py
```

## Run unit tests
1. From the location `{repo_directory}/opentripmap_data`
2. Execute `pytest-v`
3. Check all unit tests have status `PASSED`
```shell
========= test session starts =================================

rootdir: /Users/aleixparadell/assessments/ameinitz/ameinitz-data-engineering-challenge/opentripmap_data
collected 8 items

tests/test_data_extraction.py::test_parse_opentripmap_entry_fully_populated PASSED                                                                                                                                               [ 12%]
tests/test_data_extraction.py::test_parse_opentripmap_entry_no_wikidata PASSED                                                                                                                                                   [ 25%]
tests/test_data_extraction.py::test_parse_opentripmap_entry_no_osm PASSED                                                                                                                                                        [ 37%]
tests/test_data_extraction.py::test_parse_opentripmap_entry_no_osm_nor_wikidata PASSED                                                                                                                                           [ 50%]
tests/test_data_transformation.py::test_add_kinds_amount_column PASSED                                                                                                                                                           [ 62%]
tests/test_data_transformation.py::test_add_kinds_amount_column_empty_string PASSED                                                                                                                                              [ 75%]
tests/test_data_transformation.py::test_filter_by_skyscrappers_accommodations PASSED                                                                                                                                             [ 87%]
tests/test_data_transformation.py::test_filter_by_skyscrappers_accommodations_no_skyscrapper PASSED                                                                                                                              [100%]

========= 8 passed in 0.55s =================================
```

## Run the code
1. From the location `{repo_directory}/opentripmap_data`
2. Execute `python data_pipeline.py`
3. Check all the steps are executed correctly
```shell
Something went wrong, expected 2500 entries but got 500.
Data extraction ended
Data Transformation started
Data Transformation ended
Storing skyscrapper data in output_files/skyscrappers_bcn_2023-02-21.csv
File output_files/skyscrappers_bcn_2023-02-21.csv created successfully
```
4. Check output files in `opentripmap_data/output_files` directory are correctly created
