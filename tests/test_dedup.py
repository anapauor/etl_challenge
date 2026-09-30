from datetime import date

from src.quality import pick_employee_winner


def test_id_1008_later_file_date_wins():
    day_01 = {
        "id": 1008,
        "name": "Daniel Wilson",
        "source_file_date": date(2021, 1, 1),
        "source_file": "hired_employees_2021_01_01.csv",
    }
    day_02 = {
        "id": 1008,
        "name": "Daniel Wilson Updated",
        "source_file_date": date(2021, 1, 2),
        "source_file": "hired_employees_2021_01_02.csv",
    }
    winner = pick_employee_winner([day_01, day_02])
    assert winner["name"] == "Daniel Wilson Updated"
    assert winner["source_file"] == "hired_employees_2021_01_02.csv"
