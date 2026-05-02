from ortools.sat.python import cp_model
import re
import time
import random

# Maps the short disp_role keys in the JSON to the full names used in disp_demand
DISP_ROLE_TO_DEMAND_KEY = {
    "mgr": "Dispatch Manager",
    "norm": "Dispatcher",
    "trainee": "Trainee Dispatcher",
}

def build_and_solve(data):
    # start timer to include constraint construction time
    cpsat_start = time.time()

    if isinstance(data, tuple):
        data = data[0]

    global volunteer, availability, qual, date, base, role, demand
    global ctrl_volunteer, ctrl_availability, ctrl_qual, ctrl_week
    global disp_volunteer, disp_availability, disp_qual, disp_week, disp_role
    global heli_volunteer, heli_qual, heli_week, weeks_to_weekends
    global weekend_map

    volunteer = data.get("volunteer", [])
    availability = data.get("availability", {})
    qual = data.get("qual", {})
    date = data.get("date", [])
    base = data.get("base", [])
    role = data.get("role", [])
    demand  = data.get("demand", {})

    ctrl_volunteer = data.get("ctrl_volunteer", [])
    ctrl_availability = data.get("ctrl_availability", {})
    ctrl_qual = data.get("ctrl_qual", {})
    ctrl_week = data.get("ctrl_week", [])

    disp_volunteer = data.get("disp_volunteer", [])
    disp_availability = data.get("disp_availability", {})
    disp_qual = data.get("disp_qual", {})
    disp_week = data.get("disp_week", [])
    disp_role = data.get("disp_role", [])

    heli_volunteer = data.get("heli_volunteer", [])
    heli_qual = data.get("heli_qual", {})
    heli_week = data.get("heli_week", [])

    weeks_to_weekends = data.get("weeks_to_weekends", {})
    weekend_map = data.get("weekend_map", {})

    model = cp_model.CpModel()

    # main vws variable
    x = {}
    for v in volunteer:
        for d in date:
            for b in base:
                for r in role:
                    if (availability.get(v, {}).get(d, False)
                        and base_eligibility(v, b, qual)):
                        x[v, d, b, r] = model.NewBoolVar(f"x[{v},{d},{b},{r}]")

    # helitack variable
    x_heli = {}
    for v in heli_volunteer:
        v_role = heli_qual.get(v, {}).get("role")
        if v_role:
            for d in date:
                x_heli[v, d, v_role] = model.NewBoolVar(f"heli[{v},{d},{v_role}]")

    # control variable
    x_ctrl = {}
    for v in ctrl_volunteer:
        for d in ctrl_week:
            if ctrl_availability.get(v, {}).get(d, False):
                x_ctrl[v, d] = model.NewBoolVar(f"ctrl[{v},{d}]")

    # dispatch variable
    x_disp = {}
    for v in disp_volunteer:
        for d in disp_week:
            for r in disp_role:
                if disp_availability.get(v, {}).get(d, False):
                    x_disp[v, d, r] = model.NewBoolVar(f"disp[{v},{d},{r}]")

    hard_constraints(model, data, x, x_ctrl, x_disp, x_heli)
    soft_constraints(model, data, x, x_ctrl, x_disp, x_heli)

    # run solver and time it separately
    solver = cp_model.CpSolver()
    solve_start = time.time()
    status = solver.Solve(model)
    cpsat_end = time.time()

    cpsat_elapsed = cpsat_end - cpsat_start
    solve_elapsed = cpsat_end - solve_start

    if status == cp_model.OPTIMAL:
        print(f"CP-SAT status: OPTIMAL  (build+solve: {cpsat_elapsed:.2f}s | solve only: {solve_elapsed:.2f}s)")
    elif status == cp_model.FEASIBLE:
        print(f"CP-SAT status: FEASIBLE  (build+solve: {cpsat_elapsed:.2f}s | solve only: {solve_elapsed:.2f}s)")
    elif status == cp_model.INFEASIBLE:
        print(f"CP-SAT status: INFEASIBLE  (build+solve: {cpsat_elapsed:.2f}s | solve only: {solve_elapsed:.2f}s)")
        diagnose_infeasibility(data, x, x_heli, x_ctrl, x_disp)
    else:
        print(f"CP-SAT status: {solver.StatusName(status)}  (build+solve: {cpsat_elapsed:.2f}s | solve only: {solve_elapsed:.2f}s)")

    # extract and print CP-SAT solution stats
    try:
        assigned_vws, assigned_ctrl, assigned_disp, assigned_heli = _extract_assignments(
            solver, x, x_ctrl, x_disp, x_heli, data
        )
        totals = _compute_totals(assigned_vws, assigned_ctrl, assigned_disp, assigned_heli)
        aw_counts, wids, _ = _compute_assigned_week_counts(
            assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data
        )
        consec = _compute_consec_list(aw_counts, wids)
        burnout_vols_initial = _compute_burnout_volunteer_list(
            assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data
        )

        print("\n--- CP-SAT solution (before heuristic) ---")
        print("Shifts per volunteer (initial):")
        for v in volunteer:
            print(f"  {v}: {totals.get(v, 0)}")

        if consec:
            print(f"\nVolunteers with consecutive weekends (initial): {len(set(v for v,_,_ in consec))}")
            for v, w1, w2 in consec:
                print(f"  {v}: {w1} & {w2}")
        else:
            print("\nNo consecutive weekend assignments (initial).")

        if burnout_vols_initial:
            print(f"\nVolunteers at risk of burnout (initial): {len(burnout_vols_initial)}")
            for v, count in burnout_vols_initial:
                print(f"  {v}: {count} shifts in a 2-month window (cap=3)")
        else:
            print("\nNo burnout violations (initial).")
    except Exception as e:
        print(f"[Warning] Could not print initial stats: {e}")

    # perform swap-based local search
    heuristic_start = time.time()
    try:
        improved_totals, improved_consec, improved_burnout_vols = local_search_swap(
            solver, x, x_ctrl, x_disp, x_heli, data, max_iters=2000, time_limit=5
        )
    except Exception as e:
        print(f"[Warning] Local search failed: {e}")
        improved_totals, improved_consec, improved_burnout_vols = totals, consec, burnout_vols_initial
    heuristic_end = time.time()

    heuristic_elapsed = heuristic_end - heuristic_start
    total_elapsed = heuristic_end - cpsat_start

    print(f"\n--- Timing summary ---")
    print(f"  CP-SAT (build + solve): {cpsat_elapsed:.2f}s")
    print(f"  CP-SAT (solve only):    {solve_elapsed:.2f}s")
    print(f"  Heuristic (local search): {heuristic_elapsed:.2f}s")
    print(f"  Total:                  {total_elapsed:.2f}s")

    print("\n--- Heuristic solution (after local search) ---")
    print("Shifts per volunteer (after heuristic):")
    for v in volunteer:
        print(f"  {v}: {improved_totals.get(v, 0)}")

    n_consec_vols = len(set(v for v, _, _ in improved_consec)) if improved_consec else 0
    if improved_consec:
        print(f"\nVolunteers working two consecutive weekends (after heuristic): {n_consec_vols}")
        for v, w1, w2 in improved_consec:
            print(f"  {v}: {w1} & {w2}")
    else:
        print(f"\nVolunteers working two consecutive weekends (after heuristic): 0")

    n_burnout_vols = len(improved_burnout_vols) if improved_burnout_vols else 0
    if improved_burnout_vols:
        print(f"\nVolunteers at risk of burnout (after heuristic): {n_burnout_vols}")
        for v, count in improved_burnout_vols:
            print(f"  {v}: {count} shifts in a 2-month window (cap=3)")
    else:
        print(f"\nVolunteers at risk of burnout (after heuristic): 0")

    return solver, status, x, x_heli, x_ctrl, x_disp, model


def base_eligibility(v, b, qual):
    def _norm(name):
        if name is None:
            return None
        s = str(name).strip().upper()
        sub = {
            "NWL HC1": "NWL_HC1", "NWL HC2": "NWL_HC2", "NWL SU": "NWL_SU",
            "SPS BP": "SPS_BP", "SPS HC": "SPS_HC", "HDB HC/SU": "HDB_HC",
            "STB HC": "STB_HC", "HELITACK STB": "HELITACK_STB",
        }
        # BUG FIX: was missing 'return s' for names not in sub, causing None returns
        return sub.get(s, s)

    PRIMARY_TO_SUBCREWS = {
        "HDB": ["HDB_HC"],
        "STB": ["STB_HC"],
        "NWL": ["NWL_HC1", "NWL_HC2", "NWL_SU"],
        "SPS": ["SPS_HC", "SPS_BP"],
    }

    def _expand_entry(e):
        """Yield individual base strings from an entry, splitting on '/'."""
        if e is None:
            return
        parts = str(e).split("/")
        for p in parts:
            p = p.strip()
            if p:
                yield p

    v_qual = qual.get(v, {}) if isinstance(qual, dict) else {}
    entries = []
    primary   = v_qual.get("home_base")
    secondary = v_qual.get("secondary_base")
    tertiary  = v_qual.get("tertiary_base")

    for e in (primary, secondary, tertiary):
        if e is None:
            continue
        if isinstance(e, (list, tuple, set)):
            for item in e:
                entries.extend(_expand_entry(item))
        else:
            entries.extend(_expand_entry(e))

    allowed = set()
    for e in entries:
        norm = _norm(e)
        if norm:
            allowed.add(norm)
        e_up = str(e).strip().upper()
        if e_up in PRIMARY_TO_SUBCREWS:
            for sub in PRIMARY_TO_SUBCREWS[e_up]:
                allowed.add(_norm(sub))

    target = _norm(b)
    return target in allowed


# helper to check if a helitack role variable is compatible with a demanded heli role
def _compatible(var_role, demand_role):
    """Return True if var_role can satisfy demand_role.

    Compatibility rules:
    - exact match
    - FF2YR can satisfy FF demand (fallback)
    """
    if var_role is None or demand_role is None:
        return False
    vr = str(var_role)
    dr = str(demand_role)
    if vr == dr:
        return True
    # allow FF2YR to cover FF demand
    if dr == "FF" and vr == "FF2YR":
        return True
    return False


def hard_constraints(model, data, x, x_ctrl, x_disp, x_heli):
    demand_general(model, x)
    one_shift_per_weekend(model, x)
    demand_heli(model, data, x_heli)
    demand_contr_disp(model, data, x_ctrl, x_disp)
    no_overlap(model, data, x, x_ctrl, x_disp)
    trainee_with_senior(model, x)


def demand_general(model, x):
    for d in date:
        for b in base:
            for r in role:
                required = int(demand.get(b, {}).get(d, {}).get(r) or 0)
                assigned = [
                    x[v, d, b, r]
                    for v in volunteer
                    if (v, d, b, r) in x
                ]
                model.Add(sum(assigned) == required)


def demand_heli(model, data, x_heli):
    heli_demand = data.get("heli_demand", {})
    heli_dates = data.get("heli_week", date)

    for d in heli_dates:
        role_map = heli_demand.get(d, {})
        for heli_role, required in role_map.items():
            required = int(required or 0)
            if required == 0:
                continue

            assigned = [
                var for (v_key, d_key, r_key), var in x_heli.items()
                if d_key == d and _compatible(r_key, heli_role)
            ]
            model.Add(sum(assigned) == required)


def demand_contr_disp(model, data, x_ctrl, x_disp):
    """Control and dispatch demand constraints."""
    ctrl_demand = data.get("ctrl_demand", {})

    for w in data.get("ctrl_week", []):
        required = int(ctrl_demand.get(w, {}).get("Control") or 0)
        assigned_ctrl = [
            x_ctrl[v, w]
            for v in data.get("ctrl_volunteer", [])
            if (v, w) in x_ctrl
        ]
        model.Add(sum(assigned_ctrl) == required)

    disp_demand = data.get("disp_demand", {})

    for d in data.get("disp_week", []):
        for r in data.get("disp_role", []):
            demand_key = DISP_ROLE_TO_DEMAND_KEY.get(r, r)
            required = int(disp_demand.get(d, {}).get(demand_key) or 0)
            assigned = [
                x_disp[v, d, r]
                for v in data.get("disp_volunteer", [])
                if (v, d, r) in x_disp
            ]
            if str(r).lower() == "trainee":
                model.Add(sum(assigned) <= required)
            else:
                model.Add(sum(assigned) == required)


def no_overlap(model, data, x, x_ctrl, x_disp):
    # ctrl overlap
    for v in ctrl_volunteer:
        if v not in data.get("ctrl_qual", {}):
            continue
        for w in data.get("ctrl_week", []):
            if (v, w) not in x_ctrl:
                continue
            overlapping_dates = weeks_to_weekends.get(w, [])
            vws_assignments = [
                x[v, d, b, r]
                for d in overlapping_dates
                for b in base
                for r in role
                if (v, d, b, r) in x
            ]
            if vws_assignments:
                model.Add(sum(vws_assignments) + x_ctrl[v, w] <= 1)

    # dispatch overlap
    for v in disp_volunteer:
        if v not in data.get("disp_qual", {}):
            continue
        for w in data.get("disp_week", []):
            overlapping_dates = weeks_to_weekends.get(w, [])
            vws_assignments = [
                x[v, d, b, r]
                for d in overlapping_dates
                for b in base
                for r in role
                if (v, d, b, r) in x
            ]
            for r in disp_role:
                if (v, w, r) not in x_disp:
                    continue
                if vws_assignments:
                    model.Add(sum(vws_assignments) + x_disp[v, w, r] <= 1)


def trainee_with_senior(model, x):
    """Ensure at least one senior is assigned whenever a trainee is assigned."""
    for (v, d, b, r), v_var in list(x.items()):
        trainee_roles = qual.get(v, {}).get('trainee_roles', []) or []
        trainee_roles = [str(tr) for tr in trainee_roles]
        if str(r) not in trainee_roles:
            continue

        senior_vars = []
        for u in volunteer:
            if u == v:
                continue
            for r2 in role:
                if str(r2) == str(r):
                    continue
                if (u, d, b, r2) not in x:
                    continue
                u_roles = [str(rr) for rr in (qual.get(u, {}).get('role') or [])]
                u_trainee_roles = [str(tr) for tr in (qual.get(u, {}).get('trainee_roles') or [])]
                if str(r2) in u_roles and str(r2) not in u_trainee_roles:
                    senior_vars.append(x[u, d, b, r2])

        model.Add(sum(senior_vars) >= v_var)


def one_shift_per_weekend(model, x):
    wm = weekend_map or {}
    weekends = {}
    for d in date:
        wid = wm.get(d)
        if not wid:
            continue
        weekends.setdefault(wid, []).append(d)

    for wid, dates in weekends.items():
        if not dates:
            continue
        for v in volunteer:
            assigns = [
                x[v, d, b, r]
                for d in dates
                for b in base
                for r in role
                if (v, d, b, r) in x
            ]
            if assigns:
                model.Add(sum(assigns) <= 1)

def soft_constraints(model, data, x, x_ctrl, x_disp, x_heli):
    shifts_penalty = distribute_shifts_equally(model, x, x_ctrl, x_disp, x_heli)

    objective_terms = []
    if shifts_penalty is not None:
        objective_terms.append(shifts_penalty)

    if objective_terms:
        model.Minimize(sum(objective_terms))


def distribute_shifts_equally(model, x, x_ctrl, x_disp, x_heli):
    max_vws = (len(date) * len(base) * len(role)) if (date and base and role) else 0
    max_ctrl = len(ctrl_week) if ctrl_week else 0
    max_disp = (len(disp_week) * len(disp_role)) if (disp_week and disp_role) else 0
    max_heli = len(heli_week) if heli_week else 0
    max_possible = max_vws + max_ctrl + max_disp + max_heli

    total_shifts = {}
    for v in volunteer:
        terms = []
        for (vv, dd, bb, rr), var in x.items():
            if vv == v:
                terms.append(var)
        for (vv, w), var in x_ctrl.items():
            if vv == v:
                terms.append(var)
        for (vv, w, r), var in x_disp.items():
            if vv == v:
                terms.append(var)
        for (vv, dd, r), var in x_heli.items():
            if vv == v:
                terms.append(var)

        total_shifts[v] = model.NewIntVar(0, max_possible, f"total_shifts[{v}]")
        if terms:
            model.Add(total_shifts[v] == sum(terms))
        else:
            model.Add(total_shifts[v] == 0)

    max_shifts = model.NewIntVar(0, max_possible, "max_shifts")
    min_shifts = model.NewIntVar(0, max_possible, "min_shifts")
    for v in volunteer:
        model.Add(max_shifts >= total_shifts[v])
        model.Add(min_shifts <= total_shifts[v])

    range_shifts = model.NewIntVar(0, max_possible, "range_shifts")
    model.Add(range_shifts == max_shifts - min_shifts)

    global last_total_shifts
    last_total_shifts = total_shifts

    return range_shifts


def _extract_assignments(solver, x, x_ctrl, x_disp, x_heli, data):
    assigned_vws = set()
    for key, var in x.items():
        try:
            if solver.Value(var):
                assigned_vws.add(key)
        except Exception:
            pass

    assigned_ctrl = set()
    for key, var in x_ctrl.items():
        try:
            if solver.Value(var):
                assigned_ctrl.add(key)
        except Exception:
            pass

    assigned_disp = set()
    for key, var in x_disp.items():
        try:
            if solver.Value(var):
                assigned_disp.add(key)
        except Exception:
            pass

    assigned_heli = set()
    for key, var in x_heli.items():
        try:
            if solver.Value(var):
                assigned_heli.add(key)
        except Exception:
            pass

    return assigned_vws, assigned_ctrl, assigned_disp, assigned_heli


def _compute_totals(assigned_vws, assigned_ctrl, assigned_disp, assigned_heli):
    totals = {v: 0 for v in volunteer}
    for (v, d, b, r) in assigned_vws:
        if v in totals:
            totals[v] += 1
    for (v, w) in assigned_ctrl:
        if v in totals:
            totals[v] += 1
    for (v, w, r) in assigned_disp:
        if v in totals:
            totals[v] += 1
    for (v, d, r) in assigned_heli:
        if v in totals:
            totals[v] += 1
    return totals


def _compute_assigned_week_counts(assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data):
    w2w = data.get("weeks_to_weekends", {}) or {}
    date_to_wid = {}
    for wid, dates in w2w.items():
        for d in dates:
            date_to_wid[d] = wid

    # BUG FIX: include ctrl_week and disp_week in the set of week identifiers
    all_wids = set(w2w.keys())
    all_wids.update(data.get("ctrl_week", []))
    all_wids.update(data.get("disp_week", []))
    wids = sorted(list(all_wids))

    aw_counts = {(v, wid): 0 for v in volunteer for wid in wids}

    for (v, d, b, r) in assigned_vws:
        wid = date_to_wid.get(d)
        if wid is not None and (v, wid) in aw_counts:
            aw_counts[(v, wid)] += 1
    for (v, d, r) in assigned_heli:
        wid = date_to_wid.get(d)
        if wid is not None and (v, wid) in aw_counts:
            aw_counts[(v, wid)] += 1
    # ctrl/disp use their week id directly
    for (v, w) in assigned_ctrl:
        if (v, w) in aw_counts:
            aw_counts[(v, w)] += 1
    for (v, w, r) in assigned_disp:
        if (v, w) in aw_counts:
            aw_counts[(v, w)] += 1

    return aw_counts, wids, date_to_wid


def _compute_consec_list(aw_counts, wids):
    consec = []
    for v in volunteer:
        for i in range(len(wids) - 1):
            if aw_counts.get((v, wids[i]), 0) > 0 and aw_counts.get((v, wids[i + 1]), 0) > 0:
                consec.append((v, wids[i], wids[i + 1]))
    return consec


def _compute_burnout_violations(assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data, window_days=62):
    # Build mapping from date string to an integer day index.
    dates = list(data.get('date', []))
    date_to_day = {}
    parsed = True
    for d in dates:
        day = None
        for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%d/%m/%Y', '%Y%m%d'):
            try:
                t = time.strptime(str(d), fmt)
                day = int(time.mktime(t) // 86400)
                break
            except Exception:
                continue
        if day is None:
            parsed = False
            break
        date_to_day[d] = day
    if not parsed:
        date_to_day = {d: i for i, d in enumerate(dates)}

    # Map week ids (ctrl/disp) to an approximate day (earliest date in that week).
    w2w = data.get('weeks_to_weekends', {}) or {}
    week_to_day = {}
    for wid, ds in w2w.items():
        days = [date_to_day[d] for d in ds if d in date_to_day]
        if days:
            week_to_day[wid] = min(days)

    # NOTE: Do not fabricate proxy days for weeks not present in weeks_to_weekends.
    # If a ctrl/disp week id is missing from weeks_to_weekends, we can't place it on
    # a real timeline reliably, so we exclude it from the rolling-window burnout metric.

    # Collect UNIQUE shift-days per volunteer (avoid counting multiple roles on same day).
    # Burnout cap applies to general VWS shifts only.
    # Collect UNIQUE VWS shift-days per volunteer.
    v_dayset = {v: set() for v in volunteer}
    for (v, d, b, r) in assigned_vws:
        if d in date_to_day:
            v_dayset.setdefault(v, set()).add(date_to_day[d])

    # Compute max overload in any rolling window without double-counting.
    total_violation = 0
    for v, dayset in v_dayset.items():
        if not dayset:
            continue
        days_sorted = sorted(dayset)
        i = 0
        for j in range(len(days_sorted)):
            while days_sorted[j] - days_sorted[i] > window_days:
                i += 1
            count = j - i + 1
            if count > 3:
                # count only the amount above cap at this window end
                total_violation += (count - 3)
    return total_violation


def _compute_burnout_volunteer_list(assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data, window_days=62, cap=3):
    dates = list(data.get('date', []))
    date_to_day = {}
    parsed = True
    for d in dates:
        day = None
        for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%d/%m/%Y', '%Y%m%d'):
            try:
                t = time.strptime(str(d), fmt)
                day = int(time.mktime(t) // 86400)
                break
            except Exception:
                continue
        if day is None:
            parsed = False
            break
        date_to_day[d] = day
    if not parsed:
        date_to_day = {d: i for i, d in enumerate(dates)}

    # Burnout cap applies to general VWS shifts only.
    v_dayset = {v: set() for v in volunteer}
    for (v, d, b, r) in assigned_vws:
        if d in date_to_day:
            v_dayset.setdefault(v, set()).add(date_to_day[d])

    burnout_vols = []
    for v, dayset in v_dayset.items():
        if not dayset:
            continue
        days_sorted = sorted(dayset)
        max_count = 0
        i = 0
        for j in range(len(days_sorted)):
            while days_sorted[j] - days_sorted[i] > window_days:
                i += 1
            count = j - i + 1
            if count > max_count:
                max_count = count
        if max_count > cap:
            burnout_vols.append((v, max_count))

    return sorted(burnout_vols, key=lambda x: -x[1])


def validate_solution(assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data):
    # 1) per-volunteer per-date: at most one vws assignment
    date_counts = {}
    for (v, d, b, r) in assigned_vws:
        key = (v, d)
        date_counts[key] = date_counts.get(key, 0) + 1
        if date_counts[key] > 1:
            return False

    # 2) no_overlap w.r.t control and dispatch
    w2w = data.get('weeks_to_weekends', {}) or {}
    for (v, w) in assigned_ctrl:
        overlapping_dates = w2w.get(w, [])
        for (vv, d, b, r) in assigned_vws:
            if vv == v and d in overlapping_dates:
                return False
    for (v, w, r) in assigned_disp:
        overlapping_dates = w2w.get(w, [])
        for (vv, d, b, rr) in assigned_vws:
            if vv == v and d in overlapping_dates:
                return False

    # 3) one_shift_per_weekend
    wm = data.get('weekend_map', {}) or {}
    weekends = {}
    for d, wid in wm.items():
        weekends.setdefault(wid, []).append(d)
    for wid, dates in weekends.items():
        for v in volunteer:
            cnt = sum(1 for (vv, d, b, r) in assigned_vws if vv == v and d in dates)
            if cnt > 1:
                return False

    # 4) trainee_with_senior
    for (v, d, b, r) in assigned_vws:
        trainee_roles = qual.get(v, {}).get('trainee_roles', []) or []
        trainee_roles = [str(tr) for tr in trainee_roles]
        if str(r) not in trainee_roles:
            continue
        found_senior = False
        for (u, dd, bb, r2) in assigned_vws:
            if u == v or dd != d or bb != b:
                continue
            u_roles = [str(rr) for rr in (qual.get(u, {}).get('role') or [])]
            u_trainee_roles = [str(tr) for tr in (qual.get(u, {}).get('trainee_roles') or [])]
            if str(r2) in u_roles and str(r2) not in u_trainee_roles:
                found_senior = True
                break
        if not found_senior:
            return False

    return True


def local_search_swap(solver, x, x_ctrl, x_disp, x_heli, data, max_iters=1000, time_limit=5):
    """Perform swap-based local search over vws assignments only.

    Returns (improved_totals dict, consecutive pairs list, burnout volunteer list).
    """
    assigned_vws, assigned_ctrl, assigned_disp, assigned_heli = _extract_assignments(
        solver, x, x_ctrl, x_disp, x_heli, data
    )
    totals = _compute_totals(assigned_vws, assigned_ctrl, assigned_disp, assigned_heli)
    aw_counts, wids, date_to_wid = _compute_assigned_week_counts(
        assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data
    )
    consec_list = _compute_consec_list(aw_counts, wids)

    if not assigned_vws:
        burnout_vols = _compute_burnout_volunteer_list(
            assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data
        )
        return totals, consec_list, burnout_vols

    cur_burnout = _compute_burnout_violations(
        assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data
    )

    def current_metric(consec_list, burnout):
        # lexicographic: fewer consecutive weekends first, then fewer burnout violations
        return (len(consec_list), burnout)

    cur_metric = current_metric(consec_list, cur_burnout)
    assigned_vws_list = list(assigned_vws)
    start_time = time.time()

    for it in range(max_iters):
        if time_limit is not None and (time.time() - start_time) > float(time_limit):
            break
        if len(assigned_vws_list) < 2:
            break

        s1, s2 = random.sample(assigned_vws_list, 2)
        v1, d1, b1, r1 = s1
        v2, d2, b2, r2 = s2
        if v1 == v2:
            continue

        # require volunteers eligible and available for swapped slots
        if not availability.get(v1, {}).get(d2, False):
            continue
        if not availability.get(v2, {}).get(d1, False):
            continue
        if not base_eligibility(v1, b2, qual):
            continue
        if not base_eligibility(v2, b1, qual):
            continue

        wid1 = date_to_wid.get(d1)
        wid2 = date_to_wid.get(d2)

        # simulate new per-week counts and guard against double-booking a weekend
        aw1_v1 = aw_counts.get((v1, wid1), 0) - 1
        aw2_v1 = aw_counts.get((v1, wid2), 0) + 1
        aw1_v2 = aw_counts.get((v2, wid1), 0) + 1
        aw2_v2 = aw_counts.get((v2, wid2), 0) - 1

        if (wid1 is not None and aw1_v1 > 1) or (wid2 is not None and aw2_v1 > 1):
            continue
        if (wid1 is not None and aw1_v2 > 1) or (wid2 is not None and aw2_v2 > 1):
            continue

        new_assigned_vws = set(assigned_vws)
        new_assigned_vws.remove(s1)
        new_assigned_vws.remove(s2)
        new_assigned_vws.add((v1, d2, b2, r2))
        new_assigned_vws.add((v2, d1, b1, r1))

        if not validate_solution(new_assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data):
            continue

        new_totals = _compute_totals(new_assigned_vws, assigned_ctrl, assigned_disp, assigned_heli)

        new_aw = aw_counts.copy()
        if wid1 is not None:
            new_aw[(v1, wid1)] = new_aw.get((v1, wid1), 0) - 1
            new_aw[(v2, wid1)] = new_aw.get((v2, wid1), 0) + 1
        if wid2 is not None:
            new_aw[(v2, wid2)] = new_aw.get((v2, wid2), 0) - 1
            new_aw[(v1, wid2)] = new_aw.get((v1, wid2), 0) + 1

        new_consec = _compute_consec_list(new_aw, wids)
        new_burnout = _compute_burnout_violations(
            new_assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data
        )
        new_metric = current_metric(new_consec, new_burnout)

        if new_metric < cur_metric:
            assigned_vws.remove(s1)
            assigned_vws.remove(s2)
            assigned_vws.add((v1, d2, b2, r2))
            assigned_vws.add((v2, d1, b1, r1))
            totals = new_totals
            aw_counts = new_aw
            consec_list = new_consec
            cur_metric = new_metric
            assigned_vws_list = list(assigned_vws)

    burnout_vols = _compute_burnout_volunteer_list(
        assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data
    )
    return totals, consec_list, burnout_vols