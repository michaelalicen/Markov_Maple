# VWS Volunteer Rostering – Methods of Operations Research Project 1

## Project Description

Volunteer Wildfire Services (VWS), also known as the Flamebusters, is a volunteer organisation of more than 350 members contracted by CapeNature to manage wildfire response across four bases in the Western Cape: Newlands, Southern Peninsula, Stellenbosch, and Helderberg.

Each fire season (November to April), VWS must generate a standby roster assigning qualified volunteers to shifts across 48 weekends. Currently this is done manually and takes approximately one week. The goal of this project is to develop a **decision support tool** that automates roster generation subject to volunteer availability, qualifications, role requirements, and fairness constraints.

The core challenge is a **multi-skill volunteer rostering problem**: scarce qualified roles (such as Crew Leaders, Skid Drivers, and Planning) must be filled first, volunteers must not be overloaded, and the schedule must be as fair and practical as possible.

## Problem Breakdown

See [`ProblemBreakdown.pdf`](./resources/ProblemBreakdown.pdf) for our group's initial meeting findings, including data analysis, modelling approach, and key constraints.

## Deliverables

- Group oral presentation (± 20 minutes) — due **8 May 2026**
- Group technical report (max 15 pages) — due **10 May 2026**
- Individual reflective journal
- Individual peer evaluation

## Project Period

10 April 2026 – 10 May 2026

---

## Installation

Install the required dependencies with:

```bash
make install
```

This installs `ortools` (the CP-SAT solver) and `openpyxl` (for Excel output).

---

## Running the Program

All commands should be run from the project root (`Markov_Maple/`).

### Quick start

```bash
make solve
```

Runs the CP-SAT solver against the pre-cleaned JSON data and produces the roster workbook.

### Solve + full diagnostic audit

```bash
make diagnose
```

Reruns the solver and performs a full audit of whether the `OutputFormatter` placed every assignment correctly. Useful for verifying correctness. Output is captured to `output/diagnose_log.txt`; only a summary prints to the terminal.

### Audit without resolving

```bash
make audit-only
```

Audits an already-produced workbook against the raw assignment CSV from a previous `make diagnose` run. No solving is performed, so this is fast. Requires that `make diagnose` has been run at least once before.

### Validate input data

```bash
make check
```

Confirms that the input JSON exists and is well-formed, and prints a quick summary (number of volunteers, dates, and crews).

### Available commands

```bash
make help
```

---

## Output

After running `make solve` or `make diagnose`, the solver produces an **Excel workbook** at:

```
output/workbook_with_roster.xlsx
```

This file contains the completed standby roster with volunteers assigned to shifts across all 48 weekends, ready for review or distribution.

---

## Data Cleaning

The data cleaning script (`code/data_clean/data_cleaning.py`) is **not currently active**. It has been commented out of the pipeline because, while the cleaning logic is largely correct, it incorrectly maps the demand figures for some roles.

For a future project, this is straightforward to resolve — the mistakes can be identified and corrected by hand in the cleaned output. Once fixed, the script can be re-enabled by uncommenting the `clean-data` target in the `Makefile`.

The pre-cleaned JSON (`data/vws_data.json`) used by the solver is already available in the repository, so this does not affect the ability to run the program.