# VWS Rostering — Makefile
# Run all commands from the project root (Markov_Maple/)

PYTHON          := python3
JSON_OUT        := data/vws_data.json
CLEANING_SCRIPT := code/data_clean/data_cleaning.py
WORKBOOK        := data/workbook_clean.xlsx
SOLVER_SCRIPT   := code/src/Main.py

# ── default: show available commands ────────────────────────────────────────
.PHONY: help
help:
	@echo ""
	@echo "  VWS Rostering — available commands"
	@echo ""
	@echo "  make clean-data   run data_cleaning.py → produces data/vws_data.json"
	@echo "  make solve        run the CP-SAT solver  (reads data/vws_data.json)"
	@echo "  make all          clean-data then solve"
	@echo "  make check        validate JSON exists and is readable"
	@echo ""

# ── data cleaning ────────────────────────────────────────────────────────────
.PHONY: clean-data
clean-data:
	@echo "→ Running data cleaning..."
	$(PYTHON) $(CLEANING_SCRIPT) $(WORKBOOK) $(JSON_OUT)
	@echo "✓ JSON written to $(JSON_OUT)"

# ── solver ───────────────────────────────────────────────────────────────────
.PHONY: solve
solve: check
	@echo "→ Running solver..."
	$(PYTHON) $(SOLVER_SCRIPT) $(JSON_OUT)

# ── run both in sequence ─────────────────────────────────────────────────────
.PHONY: all
all: clean-data solve

# ── sanity check the JSON ────────────────────────────────────────────────────
.PHONY: check
check:
	@test -f $(JSON_OUT) || \
		(echo "✗ $(JSON_OUT) not found — run 'make clean-data' first" && exit 1)
	@$(PYTHON) -c "\
import json; \
d = json.load(open('$(JSON_OUT)')); \
print('✓ JSON OK —', len(d['volunteer']), 'volunteers,', len(d['date']), 'dates,', len(d['demand']), 'crews')"