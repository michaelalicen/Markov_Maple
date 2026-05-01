'''
    Pipelines the three files together
'''
import sys
import os
from Scheduler import build_and_solve
from OutputFormatter import write_roster
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data_clean'))
from data_cleaning import load_and_validate


def main():
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'workbook_clean.xlsx'))
    data = load_and_validate(data_path)
    solution = build_and_solve(data)
    #write_roster(solution, "output/roster.xlsx")

if __name__ == "__main__":
    main()