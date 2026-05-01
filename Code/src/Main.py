'''
    Pipelines the three files together
'''
from DataCleaning import load_and_validate
from code.src.Scheduler import build_and_solve
from code.src.OutputFormatter import write_roster

def main():
    data = load_and_validate("data/")
    solution = build_and_solve(data)
    write_roster(solution, "output/roster.xlsx")