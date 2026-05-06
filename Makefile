# VWS Rostering — Makefile
# Run all commands from the project root (Markov_Maple/)

PYTHON           := python3
JSON_OUT         := data/vws_data.json
CLEANING_SCRIPT  := code/data_clean/data_cleaning.py
WORKBOOK         := data/workbook_clean.xlsx
SOLVER_SCRIPT    := code/src/Main.py
DIAGNOSE_SCRIPT  := code/src/diagnose_roster_mapping.py

OUTPUT_DIR       := output
ROSTER_OUT       := $(OUTPUT_DIR)/workbook_with_roster.xlsx
AUDIT_DIR        := $(OUTPUT_DIR)/mapping_audit
RAW_CSV          := $(AUDIT_DIR)/raw_solution_all.csv
DIAGNOSE_LOG     := $(OUTPUT_DIR)/diagnose_log.txt

# ── default: show available commands ────────────────────────────────────────
.PHONY: help
help:
	@echo ""
	@echo "  VWS Rostering — available commands"
	@echo ""
	@echo "  make clean-data     run data_cleaning.py  → produces $(JSON_OUT)"
	@echo "  make solve          run CP-SAT solver      → produces $(ROSTER_OUT)"
	@echo "                                               solver log → $(OUTPUT_DIR)/solver_log.txt"
	@echo "  make diagnose       solve + audit in one go → $(DIAGNOSE_LOG)"
	@echo "                      NOTE: this reruns the solver (see README)"
	@echo "  make audit-only     audit existing workbook without resolving"
	@echo "                      requires a previous 'make diagnose' to have run"
	@echo "  make all            clean-data then solve"
	@echo "  make check          validate JSON exists and is readable"
	@echo ""

# ── data cleaning ────────────────────────────────────────────────────────────
.PHONY: clean-data
clean-data:
	@echo "→ Running data cleaning..."
	$(PYTHON) $(CLEANING_SCRIPT) $(WORKBOOK) $(JSON_OUT)
	@echo "✓ JSON written to $(JSON_OUT)"

# ── solver only ──────────────────────────────────────────────────────────────
# Diagnostic log written automatically by Main.py via logger.py
.PHONY: solve
solve: check
	@mkdir -p $(OUTPUT_DIR)
	@echo "→ Running solver..."
	$(PYTHON) $(SOLVER_SCRIPT) $(JSON_OUT)

# ── solve + full diagnostic audit (RERUNS the solver) ────────────────────────
# Use this when you want to audit whether OutputFormatter placed everything
# correctly. It resolves, saves the raw assignment CSV, writes the workbook,
# then audits the workbook against the raw CSV.
# All output captured to diagnose_log.txt — only the summary prints to terminal.
.PHONY: diagnose
diagnose: check
	@mkdir -p $(OUTPUT_DIR) $(AUDIT_DIR)
	@echo "→ Running solver + diagnostic audit (this reruns the solver)..."
	@echo "  Full output → $(DIAGNOSE_LOG)"
	$(PYTHON) $(DIAGNOSE_SCRIPT) \
		--data      $(JSON_OUT) \
		--template  $(WORKBOOK) \
		--output    $(ROSTER_OUT) \
		--audit-dir $(AUDIT_DIR) \
		> $(DIAGNOSE_LOG) 2>&1
	@echo "✓ Roster written to  $(ROSTER_OUT)"
	@echo "✓ Audit CSVs in      $(AUDIT_DIR)/"
	@echo "✓ Full log at        $(DIAGNOSE_LOG)"

# ── audit only (does NOT rerun the solver) ───────────────────────────────────
# Audits an already-produced workbook against the raw CSV from a previous
# 'make diagnose' run. Fast — no solving involved.
# Requires: $(RAW_CSV) and $(ROSTER_OUT) to already exist.
.PHONY: audit-only
audit-only:
	@test -f $(RAW_CSV) || \
		(echo "✗ $(RAW_CSV) not found — run 'make diagnose' first to produce it" && exit 1)
	@test -f $(ROSTER_OUT) || \
		(echo "✗ $(ROSTER_OUT) not found — run 'make solve' or 'make diagnose' first" && exit 1)
	@mkdir -p $(AUDIT_DIR)
	@echo "→ Auditing existing workbook (no solve)..."
	@echo "  Workbook : $(ROSTER_OUT)"
	@echo "  Raw CSV  : $(RAW_CSV)"
	$(PYTHON) $(DIAGNOSE_SCRIPT) \
		--skip-solve \
		--raw-csv   $(RAW_CSV) \
		--workbook  $(ROSTER_OUT) \
		--audit-dir $(AUDIT_DIR) \
		> $(DIAGNOSE_LOG) 2>&1
	@echo "✓ Audit CSVs in  $(AUDIT_DIR)/"
	@echo "✓ Full log at    $(DIAGNOSE_LOG)"

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