import s3fs.utils as utils


def test_get_brange():
    assert list(utils._get_brange(100, 24)) == [
        (0, 23),
        (24, 47),
        (48, 71),
        (72, 95),
        (96, 99),
    ]
    assert list(utils._get_brange(100, 25)) == [(0, 24), (25, 49), (50, 74), (75, 99)]
    assert list(utils._get_brange(100, 26)) == [(0, 25), (26, 51), (52, 77), (78, 99)]
