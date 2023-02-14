from model import Model


def test_object_len():
    test_model = Model()
    objects = test_model.get_objects()
    assert len(objects) == 500


def test_filter():
    count = 0
    test_model = Model()
    df = test_model.get_df_detailed()
    if 'skyscrapers' in df['kinds']:
        count += 1
    count == 6


