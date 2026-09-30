from src.quality import quality_reasons

DIMS = set(range(1, 6))
JOBS = set(range(1, 6))


def _row(**overrides):
    base = {
        "id": 1001,
        "name": "Ana Lopez",
        "datetime": "2021-01-15T10:00:00Z",
        "department_id": 1,
        "job_id": 2,
    }
    return {**base, **overrides}


def test_blank_name():
    assert "null_or_empty_name" in quality_reasons(_row(name="   "), DIMS, JOBS)


def test_none_name():
    assert "null_or_empty_name" in quality_reasons(_row(name=None), DIMS, JOBS)


def test_invalid_date_string():
    assert "invalid_datetime" in quality_reasons(_row(datetime="invalid_date"), DIMS, JOBS)


def test_bad_timestamp():
    assert "invalid_datetime" in quality_reasons(_row(datetime="bad_timestamp"), DIMS, JOBS)


def test_iso_z_datetime_is_valid():
    assert "invalid_datetime" not in quality_reasons(
        _row(datetime="2021-01-15T10:00:00Z"), DIMS, JOBS
    )


def test_empty_department_id():
    reasons = quality_reasons(_row(department_id=""), DIMS, JOBS)
    assert "null_department_id" in reasons
    assert "fk_department" not in reasons


def test_none_department_id():
    reasons = quality_reasons(_row(department_id=None), DIMS, JOBS)
    assert "null_department_id" in reasons
    assert "fk_department" not in reasons


def test_fk_department():
    assert "fk_department" in quality_reasons(_row(department_id=99), DIMS, JOBS)


def test_blank_name_and_bad_datetime():
    reasons = quality_reasons(_row(name="   ", datetime="bad_timestamp"), DIMS, JOBS)
    assert "null_or_empty_name" in reasons
    assert "invalid_datetime" in reasons
