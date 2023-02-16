from model import Model
from collections import Counter


def test_len_object_json():
    length = 0
    for request in Model().request_objects():
        length += len(request)
    assert length == 2500


def test_filter():
    error = 0
    df = Model().get_df_detailed()
    if 'skyscrapers' not in df['kinds']:
        error += 1
    assert error == 0


def check_for_duplicates():
    assert print(len(Counter(Model().request_objects())) - len(set(Model().request_objects()))) == 0
