from ortools.sat.python import cp_model
import re
import time
import random
import numpy as np
from logger import log_print

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

    # If any volunteer has no available dates at all, make them available on every date
    # so they can be used if needed. This runs before any decision variables are created.
    forced_available = set()
    for v in volunteer:
        av = availability.get(v, {})
        if not any(av.get(d, False) for d in (date or [])):
            availability.setdefault(v, {})
            for d in (date or []):
                availability[v][d] = True
            forced_available.add(v)
            log_print(f"[Info] Volunteer '{v}' had no VWS availability; marked available for all dates.")

    model = cp_model.CpModel()

    # main vws variable
    x = {}
    for v in volunteer:
        for d in date:
            for b in base:
                for r in role:
                    # planning is only valid at NWL_HC1 -- block it everywhere else
                    if str(r) == 'planning' and str(b) != 'NWL_HC1':
                        continue
                    if (availability.get(v, {}).get(d, False)
                        and base_eligibility(v, b, qual)):
                        x[v, d, b, r] = model.NewBoolVar(f"x[{v},{d},{b},{r}]")

    # helitack variable -- only for heli_week dates, NOT all VWS dates
    x_heli = {}
    for v in heli_volunteer:
        v_role = heli_qual.get(v, {}).get("role")
        if v_role:
            for d in heli_week:
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
    soft_constraints(model, data, x, x_ctrl, x_disp, x_heli, forced_available)

    # run solver and time it separately
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 800.0
    solver.parameters.num_search_workers = 8

    solve_start = time.time()
    status = solver.Solve(model)
    cpsat_end = time.time()

    cpsat_elapsed = cpsat_end - cpsat_start
    solve_elapsed = cpsat_end - solve_start

    _log_solver_status(status, solver, cpsat_elapsed, solve_elapsed)

    # extract and log CP-SAT solution stats
    try:
        assigned_vws, assigned_ctrl, assigned_disp, assigned_heli = get_solution_variables(
            solver, x, x_ctrl, x_disp, x_heli, data
        )
        totals = calculate_totals(assigned_vws, assigned_ctrl, assigned_disp, assigned_heli)

        # Diagnostic: how many volunteers have zero shifts?
        try:
            zero_vols = [v for v in volunteer if int(totals.get(v, 0)) == 0]
            log_print(f"\n[Diag] Volunteers with zero shifts: {len(zero_vols)} / {len(volunteer)}")
        except Exception:
            pass

        # Log NWL_HC1 planner assignments (VWS role='planning')
        try:
            planners = [(v, d) for (v, d, b, r) in assigned_vws if str(b) == 'NWL_HC1' and str(r) == 'planning']
            if planners:
                log_print("\n--- NWL_HC1 planner assignments ---")
                by_date = {}
                for v, d in planners:
                    by_date.setdefault(d, []).append(v)
                for d in (data.get('date', []) or sorted(by_date.keys())):
                    vols = sorted(by_date.get(d, []))
                    if vols:
                        log_print(f"  {d}: {', '.join(vols)}")
            else:
                log_print("\n--- NWL_HC1 planner assignments ---\n  (none)")
        except Exception:
            pass

        # Log control assignments (by control week)
        try:
            if assigned_ctrl:
                log_print("\n--- Control assignments ---")
                ctrl_by_week = {}
                for (v, w) in assigned_ctrl:
                    ctrl_by_week.setdefault(w, []).append(v)
                for w in (data.get('ctrl_week', []) or sorted(ctrl_by_week.keys())):
                    vols = sorted(ctrl_by_week.get(w, []))
                    if vols:
                        log_print(f"  {w}: {', '.join(vols)}")
            else:
                log_print("\n--- Control assignments ---\n  (none)")
        except Exception:
            pass

        # Log dispatch assignments (by dispatch week and role)
        try:
            if assigned_disp:
                log_print("\n--- Dispatch assignments ---")
                disp_by_week_role = {}
                for (v, w, r) in assigned_disp:
                    disp_by_week_role.setdefault((w, r), []).append(v)
                for w in (data.get('disp_week', []) or sorted({ww for (ww, _) in disp_by_week_role.keys()})):
                    any_printed = False
                    for r in (data.get('disp_role', []) or sorted({rr for (_, rr) in disp_by_week_role.keys()})):
                        vols = sorted(disp_by_week_role.get((w, r), []))
                        if vols:
                            if not any_printed:
                                log_print(f"  {w}:")
                                any_printed = True
                            log_print(f"    {r}: {', '.join(vols)}")
            else:
                log_print("\n--- Dispatch assignments ---\n  (none)")
        except Exception:
            pass

        aw_counts, wids, _ = _compute_assigned_week_counts(
            assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data
        )
        consec = consecutive_weekends(aw_counts, wids)
        burnout_vols_initial = burnout_cap(
            assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data
        )
        _log_solution_stats(totals, consec, burnout_vols_initial)
        _log_unpaired_requests(assigned_vws, data)
    except Exception as e:
        log_print(f"[Warning] Could not log initial stats: {e}")

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
        return sub.get(s, s)

    PRIMARY_TO_SUBCREWS = {
        "HDB": ["HDB_HC"],
        "STB": ["STB_HC"],
        "NWL": ["NWL_HC1", "NWL_HC2", "NWL_SU"],
        "SPS": ["SPS_HC", "SPS_BP"],
    }

    def _expand_entry(e):
        if e is None:
            return
        parts = str(e).split("/")
        for p in parts:
            p = p.strip()
            if p:
                yield p

    v_qual = qual.get(v, {}) if isinstance(qual, dict) else {}
    primary = v_qual.get("home_base")

    entries = []
    if primary is not None:
        if isinstance(primary, (list, tuple, set)):
            for item in primary:
                entries.extend(list(_expand_entry(item)))
        else:
            entries.extend(list(_expand_entry(primary)))

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


def _compatible(var_role, demand_role):
    if var_role is None or demand_role is None:
        return False
    vr = str(var_role)
    dr = str(demand_role)
    if vr == dr:
        return True
    if dr == "FF" and vr == "FF2YR":
        return True
    return False


def hard_constraints(model, data, x, x_ctrl, x_disp, x_heli):
    demand_general(model, x)
    for (v, d, b, r), var in x.items():
        if not availability.get(v, {}).get(d, False):
            model.Add(var == 0)

    one_shift_per_weekend(model, x)
    demand_heli(model, data, x_heli)
    demand_contr_disp(model, data, x_ctrl, x_disp)
    no_overlap(model, data, x, x_ctrl, x_disp)
    trainee_with_senior(model, x)
    require_planning_on_nwl_hc1(model, data, x)
    restrict_extra_shift_roles(model, data, x)


def restrict_extra_shift_roles(model, data, x):
    qual_local = data.get("qual", {}) or {}
    all_roles = list(role)

    for v in volunteer:
        v_qual = qual_local.get(v, {}) or {}
        extra_shifts = int(v_qual.get("extra_shifts", 0) or 0)

        if extra_shifts == 0:
            continue

        approved_roles = _parse_extra_shift_roles(
            v_qual.get("extra_shift_role", ""), all_roles
        )

        if not approved_roles:
            continue

        unapproved_vars = [
            x[v, d, b, r]
            for d in date
            for b in base
            for r in all_roles
            if (v, d, b, r) in x and r not in approved_roles
        ]

        if not unapproved_vars:
            continue

        model.Add(sum(unapproved_vars) <= 5)


def require_planning_on_nwl_hc1(model, data, x):
    demand_local = data.get('demand', {}) or {}
    nwl_hc1_demand = demand_local.get('NWL_HC1', {}) or {}

    for d, role_dict in nwl_hc1_demand.items():
        if not (role_dict or {}).get('planning'):
            continue

        senior_vars = [
            x[v, d, 'NWL_HC1', 'planning']
            for v in volunteer
            if (v, d, 'NWL_HC1', 'planning') in x
            and 'planning' in [str(r) for r in (qual.get(v, {}).get('role') or [])]
        ]

        trainee_vars = [
            x[v, d, 'NWL_HC1', 'planning']
            for v in volunteer
            if (v, d, 'NWL_HC1', 'planning') in x
            and 'planning' in [str(r) for r in (qual.get(v, {}).get('trainee_roles') or [])]
            and 'planning' not in [str(r) for r in (qual.get(v, {}).get('role') or [])]
        ]

        if senior_vars:
            model.Add(sum(senior_vars) == 1)
        else:
            log_print(f"[Warning] No eligible senior planner for NWL_HC1 on {d}; planning constraint skipped.")
            continue

        if trainee_vars:
            model.Add(sum(trainee_vars) <= 1)


SOFT_DEMAND_ROLES = {'recruit_FF', 'trainee_ACL'}
DEMAND_GENERAL_EXCLUDED_ROLES = {'planning'}

def demand_general(model, x):
    for d in date:
        for b in base:
            for r in role:
                if str(r) in DEMAND_GENERAL_EXCLUDED_ROLES:
                    continue
                required = int(demand.get(b, {}).get(d, {}).get(r) or 0)
                assigned = [
                    x[v, d, b, r]
                    for v in volunteer
                    if (v, d, b, r) in x
                ]
                if str(r) in SOFT_DEMAND_ROLES:
                    model.Add(sum(assigned) <= required)
                else:
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

    ctrl_qual_local = data.get("ctrl_qual", {}) or {}

    ctrl_week_to_disp_weeks = {}
    for w_ctrl in data.get("ctrl_week", []) or []:
        disp_wids = set()
        for d in (weeks_to_weekends.get(w_ctrl, []) or []):
            wid = weekend_map.get(d)
            if wid:
                disp_wids.add(wid)
        if disp_wids:
            ctrl_week_to_disp_weeks[w_ctrl] = disp_wids

    for v in set(data.get("ctrl_volunteer", []) or []) | set(data.get("disp_volunteer", []) or []):
        allows_disp_control = bool((ctrl_qual_local.get(v, {}) or {}).get("disp_control", False))
        if allows_disp_control:
            continue

        for w_ctrl in data.get("ctrl_week", []) or []:
            if (v, w_ctrl) not in x_ctrl:
                continue

            overlapping_disp_weeks = ctrl_week_to_disp_weeks.get(w_ctrl, set())
            if not overlapping_disp_weeks:
                continue

            disp_terms = []
            for w_disp in overlapping_disp_weeks:
                for r in (data.get("disp_role", []) or []):
                    var = x_disp.get((v, w_disp, r))
                    if var is not None:
                        disp_terms.append(var)

            if disp_terms:
                model.Add(sum(disp_terms) + x_ctrl[v, w_ctrl] <= 1)

    for v in disp_volunteer:
        for w in data.get("disp_week", []):
            mgr = x_disp.get((v, w, "mgr"))
            norm = x_disp.get((v, w, "norm"))
            trainee = x_disp.get((v, w, "trainee"))

            if trainee is not None:
                if mgr is not None:
                    model.Add(trainee + mgr <= 1)
                if norm is not None:
                    model.Add(trainee + norm <= 1)

            role_vars = [vv for vv in (mgr, norm, trainee) if vv is not None]
            if role_vars:
                model.Add(sum(role_vars) <= 2)

            for r in disp_role:
                if r in ("mgr", "norm", "trainee"):
                    continue
                other = x_disp.get((v, w, r))
                if other is None:
                    continue
                if mgr is not None:
                    model.Add(other + mgr <= 1)
                if norm is not None:
                    model.Add(other + norm <= 1)
                if trainee is not None:
                    model.Add(other + trainee <= 1)
                model.Add(other <= 0)


def trainee_with_senior(model, x):
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


def _parse_extra_shift_roles(extra_shift_role_str, all_roles):
    if not extra_shift_role_str:
        return frozenset()

    TOKEN_MAP = {
        "ff": {"FF"},
        "driver": {"crew_driver", "skid_driver"},
        "planning": {"planning"},
        "logistics": {"logistics"},
        "acl": {"ACL"},
        "cl": {"CL"},
    }

    import re as _re
    tokens = _re.split(r"[,\s]+or[,\s]+|,", str(extra_shift_role_str), flags=_re.IGNORECASE)
    result = set()
    for tok in tokens:
        tok = tok.strip().lower()
        if tok in TOKEN_MAP:
            result.update(TOKEN_MAP[tok])

    return frozenset(r for r in result if r in all_roles)


def soft_constraints(model, data, x, x_ctrl, x_disp, x_heli, forced_available=None):
    if forced_available is None:
        forced_available = set()

    shifts_penalty, total_shifts_map = distribute_shifts_equally(model, x, x_ctrl, x_disp, x_heli)
    pair_penalties = pair_volunteers(model, data, x)

    zero_shift_terms = []
    try:
        for v, tvar in total_shifts_map.items():
            no_shift = model.NewBoolVar(f"no_shift[{v}]")
            model.Add(tvar == 0).OnlyEnforceIf(no_shift)
            model.Add(tvar >= 1).OnlyEnforceIf(no_shift.Not())
            zero_shift_terms.append(no_shift)
    except Exception:
        zero_shift_terms = []

    burnout_terms = []
    try:
        window_days = int(data.get('burnout_window_days', 62) or 62)
        cap = int(data.get('burnout_cap', 3) or 3)

        dates = list(data.get('date', []) or date)
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

        ordered_dates = sorted(dates, key=lambda dd: date_to_day.get(dd, 0))
        n = len(ordered_dates)

        windows = []
        for i in range(n):
            start_day = date_to_day.get(ordered_dates[i], i)
            win = []
            for j in range(i, n):
                if date_to_day.get(ordered_dates[j], j) - start_day <= window_days:
                    win.append(ordered_dates[j])
                else:
                    break
            if win:
                windows.append((i, win))

        for v in volunteer:
            for wi, win_dates in windows:
                terms = [
                    x[v, d, b, r]
                    for d in win_dates
                    for b in base
                    for r in role
                    if (v, d, b, r) in x
                ]
                if not terms:
                    continue
                count_var = model.NewIntVar(0, len(terms), f"burn_count[{v},{wi}]")
                model.Add(count_var == sum(terms))
                excess_var = model.NewIntVar(0, len(terms), f"burn_excess[{v},{wi}]")
                model.Add(excess_var >= count_var - cap)
                model.Add(excess_var >= 0)
                burnout_terms.append(excess_var)
    except Exception:
        burnout_terms = []

    disp_overlap_terms = []
    for v in data.get("disp_volunteer", []):
        for w in data.get("disp_week", []):
            mgr = x_disp.get((v, w, "mgr"))
            norm = x_disp.get((v, w, "norm"))
            if mgr is None or norm is None:
                continue
            both = model.NewBoolVar(f"disp_mgr_norm_same[{v},{w}]")
            model.Add(both <= mgr)
            model.Add(both <= norm)
            model.Add(both >= mgr + norm - 1)
            disp_overlap_terms.append(both)

    trainee_unfilled_terms = []
    disp_demand = data.get("disp_demand", {}) or {}
    for w in data.get("disp_week", []):
        required = int(disp_demand.get(w, {}).get(DISP_ROLE_TO_DEMAND_KEY.get("trainee", "trainee")) or 0)
        if required <= 0:
            continue
        assigned = []
        for v in data.get("disp_volunteer", []):
            var = x_disp.get((v, w, "trainee"))
            if var is not None:
                assigned.append(var)
        if not assigned:
            continue
        unfilled = model.NewIntVar(0, required, f"disp_trainee_unfilled[{w}]")
        model.Add(unfilled == required - sum(assigned))
        trainee_unfilled_terms.append(unfilled)

    ctrl_excess_terms = []
    try:
        for v in data.get("ctrl_volunteer", []):
            terms = [x_ctrl[v, w] for w in data.get("ctrl_week", []) if (v, w) in x_ctrl]
            if not terms:
                continue
            t_ctrl = model.NewIntVar(0, len(terms), f"total_ctrl[{v}]")
            model.Add(t_ctrl == sum(terms))
            excess = model.NewIntVar(0, 10000, f"ctrl_excess_over_3[{v}]")
            model.Add(excess >= t_ctrl - 3)
            model.Add(excess >= 0)
            ctrl_excess_terms.append(excess)
    except Exception:
        ctrl_excess_terms = []

    disp_excess_terms = []
    try:
        for v in data.get("disp_volunteer", []):
            disp_week_terms = []
            for w in data.get("disp_week", []):
                role_vars = [x_disp.get((v, w, r)) for r in data.get("disp_role", []) if (v, w, r) in x_disp]
                role_vars = [rv for rv in role_vars if rv is not None]
                if not role_vars:
                    continue
                any_disp = model.NewBoolVar(f"disp_any_sc[{v},{w}]")
                model.Add(sum(role_vars) >= any_disp)
                model.Add(sum(role_vars) <= len(role_vars) * any_disp)
                disp_week_terms.append(any_disp)
            if not disp_week_terms:
                continue
            t_disp = model.NewIntVar(0, len(disp_week_terms), f"total_disp[{v}]")
            model.Add(t_disp == sum(disp_week_terms))
            excess = model.NewIntVar(0, 10000, f"disp_excess_over_3[{v}]")
            model.Add(excess >= t_disp - 3)
            model.Add(excess >= 0)
            disp_excess_terms.append(excess)
    except Exception:
        disp_excess_terms = []

    excess_terms = []
    try:
        qual_local = data.get("qual", {}) or {}
        for v, tvar in total_shifts_map.items():
            v_qual = qual_local.get(v, {}) or {}
            extra_allowed = int(v_qual.get("extra_shifts", 0) or 0)
            cap = 5 + extra_allowed
            excess = model.NewIntVar(0, 10000, f"excess_over_cap[{v}]")
            model.Add(excess >= tvar - cap)
            model.Add(excess >= 0)
            if extra_allowed == 0:
                excess_terms.append((excess, 10000))
            else:
                excess_terms.append((excess, 5000))
    except Exception:
        excess_terms = []

    weighted_terms = []
    if zero_shift_terms:
        weighted_terms.append((sum(zero_shift_terms), 100))
    if shifts_penalty is not None:
        weighted_terms.append((shifts_penalty, 1000))
    if disp_overlap_terms:
        weighted_terms.append((sum(disp_overlap_terms), 1000))
    if trainee_unfilled_terms:
        weighted_terms.append((sum(trainee_unfilled_terms), 10))
    if burnout_terms:
        burnout_weight = int(data.get('burnout_weight', 10) or 10)
        weighted_terms.append((sum(burnout_terms), burnout_weight))
    if excess_terms:
        weighted_terms.append((sum(expr * w for expr, w in excess_terms), 1))
    if ctrl_excess_terms:
        weighted_terms.append((sum(ctrl_excess_terms), 1000))
    if disp_excess_terms:
        weighted_terms.append((sum(disp_excess_terms), 1000))
    if pair_penalties:
        weighted_terms.append((sum(pair_penalties), 1))

    if weighted_terms:
        obj_terms = [expr * weight for expr, weight in weighted_terms]
        model.Minimize(sum(obj_terms))


def distribute_shifts_equally(model, x, x_ctrl, x_disp, x_heli):
    num_weekends = len(set(weekend_map.values())) if weekend_map else (len(date) if date else 0)
    max_vws = num_weekends
    max_ctrl = len(ctrl_week) if ctrl_week else 0
    max_disp = len(disp_week) if disp_week else 0
    max_heli = len(heli_week) if heli_week else 0
    max_possible = max(max_vws + max_ctrl + max_disp + max_heli, 1)

    disp_any = {}
    try:
        for v in volunteer:
            for w in (disp_week or []):
                role_vars = [x_disp.get((v, w, r)) for r in (disp_role or []) if (v, w, r) in x_disp]
                if not role_vars:
                    continue
                bv = model.NewBoolVar(f"disp_any[{v},{w}]")
                model.Add(sum(role_vars) >= bv)
                model.Add(sum(role_vars) <= len(role_vars) * bv)
                disp_any[(v, w)] = bv
    except Exception:
        disp_any = {}

    vws_by_vol = {}
    for (vv, dd, bb, rr), var in x.items():
        vws_by_vol.setdefault(vv, []).append(var)
    ctrl_by_vol = {}
    for (vv, w), var in x_ctrl.items():
        ctrl_by_vol.setdefault(vv, []).append(var)
    disp_any_by_vol = {}
    for (vv, w), var in disp_any.items():
        disp_any_by_vol.setdefault(vv, []).append(var)
    heli_by_vol = {}
    for (vv, dd, r), var in x_heli.items():
        heli_by_vol.setdefault(vv, []).append(var)

    total_shifts = {}
    for v in volunteer:
        terms = []
        terms.extend(vws_by_vol.get(v, []))
        terms.extend(ctrl_by_vol.get(v, []))
        terms.extend(disp_any_by_vol.get(v, []))
        terms.extend(heli_by_vol.get(v, []))

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

    return range_shifts, total_shifts


def pair_volunteers(model, data, x):
    pair_penalties = []
    pair_requests = list((data.get('pairing_requests', []) or []))

    v_db_present = {}
    for v in volunteer:
        for d in date:
            for b in base:
                vars_v = [x[v, d, b, r] for r in role if (v, d, b, r) in x]
                if not vars_v:
                    continue
                bv = model.NewBoolVar(f"in_date_base[{v},{d},{b}]")
                model.Add(sum(vars_v) >= bv)
                model.Add(sum(vars_v) <= len(vars_v) * bv)
                v_db_present[(v, d, b)] = bv

    for pair in pair_requests:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        v1, v2 = pair
        both_vars = []
        for d in date:
            for b in base:
                k1 = (v1, d, b)
                k2 = (v2, d, b)
                if k1 in v_db_present and k2 in v_db_present:
                    both = model.NewBoolVar(f"pair_both[{v1},{v2},{d},{b}]")
                    model.Add(both <= v_db_present[k1])
                    model.Add(both <= v_db_present[k2])
                    model.Add(both >= v_db_present[k1] + v_db_present[k2] - 1)
                    both_vars.append(both)
        if not both_vars:
            continue
        paired = model.NewBoolVar(f"paired[{v1},{v2}]")
        model.Add(paired <= sum(both_vars))
        for bv in both_vars:
            model.Add(paired >= bv)

        unpaired = model.NewIntVar(0, 1, f"pair_unpaired[{v1},{v2}]")
        model.Add(paired + unpaired == 1)
        pair_penalties.append(unpaired)

    return pair_penalties


def get_solution_variables(solver, x, x_ctrl, x_disp, x_heli, data):
    assigned_vws = set()
    for key, var in x.items():
        try:
            if int(solver.Value(var)) == 1:
                assigned_vws.add(key)
        except Exception:
            pass

    assigned_ctrl = set()
    for key, var in x_ctrl.items():
        try:
            if int(solver.Value(var)) == 1:
                assigned_ctrl.add(key)
        except Exception:
            pass

    assigned_disp = set()
    for key, var in x_disp.items():
        try:
            if int(solver.Value(var)) == 1:
                assigned_disp.add(key)
        except Exception:
            pass

    assigned_heli = set()
    for key, var in x_heli.items():
        try:
            if int(solver.Value(var)) == 1:
                assigned_heli.add(key)
        except Exception:
            pass

    return assigned_vws, assigned_ctrl, assigned_disp, assigned_heli


def calculate_totals(assigned_vws, assigned_ctrl, assigned_disp, assigned_heli):
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
    for (v, w) in assigned_ctrl:
        if (v, w) in aw_counts:
            aw_counts[(v, w)] += 1
    for (v, w, r) in assigned_disp:
        if (v, w) in aw_counts:
            aw_counts[(v, w)] += 1

    return aw_counts, wids, date_to_wid


def consecutive_weekends(aw_counts, wids):
    consec = []
    for v in volunteer:
        for i in range(len(wids) - 1):
            if aw_counts.get((v, wids[i]), 0) > 0 and aw_counts.get((v, wids[i + 1]), 0) > 0:
                consec.append((v, wids[i], wids[i + 1]))
    return consec


def burnout_violations(assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data, window_days=62):
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

    w2w = data.get('weeks_to_weekends', {}) or {}
    week_to_day = {}
    for wid, ds in w2w.items():
        days = [date_to_day[d] for d in ds if d in date_to_day]
        if days:
            week_to_day[wid] = min(days)

    v_dayset = {}
    for (v, d, b, r) in assigned_vws:
        if d in date_to_day:
            v_dayset.setdefault(v, set()).add(date_to_day[d])

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
                total_violation += (count - 3)
    return total_violation


def burnout_cap(assigned_vws, assigned_ctrl, assigned_disp, assigned_heli, data, window_days=62, cap=3):
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

    v_dayset = {}
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


# ---------------------------------------------------------------------------
# Internal log helpers (all go to file via log_print, never to terminal)
# ---------------------------------------------------------------------------

def _log_solver_status(status, solver, cpsat_elapsed=None, solve_elapsed=None):
    cpsat_elapsed = cpsat_elapsed or 0.0
    solve_elapsed = solve_elapsed or 0.0
    name = solver.StatusName(status)
    log_print(f"CP-SAT status: {name}  (build+solve: {cpsat_elapsed:.2f}s | solve only: {solve_elapsed:.2f}s)")


def _log_solution_stats(totals, consec, burnout_vols_initial):
    log_print("\n--- CP-SAT solution ---")
    log_print("Shifts per volunteer:")
    for v in volunteer:
        log_print(f"  {v}: {totals.get(v, 0)}")
    log_print(f"  Mean shifts per volunteer: {np.mean(list(totals.values())):.2f}")

    try:
        log_print("\nControl shifts per control volunteer:")
        for v in ctrl_volunteer:
            log_print(f"  {v}: {totals.get(v, 0)}")
        log_print("\nDispatch shifts per dispatch volunteer:")
        for v in disp_volunteer:
            log_print(f"  {v}: {totals.get(v, 0)}")
    except Exception:
        pass

    if consec:
        log_print(f"\nVolunteers with consecutive weekends: {len(set(v for v,_,_ in consec))}")
        for v, w1, w2 in consec:
            log_print(f"  {v}: {w1} & {w2}")
    else:
        log_print("\nNo consecutive weekend assignments.")

    if burnout_vols_initial:
        log_print(f"\nVolunteers at risk of burnout: {len(burnout_vols_initial)}")
        for v, count in burnout_vols_initial:
            log_print(f"  {v}: {count} shifts in a 2-month window (cap=3)")
    else:
        log_print("\nNo burnout violations.")


def _log_unpaired_requests(assigned_vws, data):
    pair_requests = list(data.get('pairing_requests', []) or [])
    if not pair_requests:
        return

    wm = data.get('weekend_map', {}) or {}
    v_to_wids = {}
    for (v, d, b, r) in assigned_vws:
        wid = wm.get(d)
        if wid is not None:
            v_to_wids.setdefault(v, set()).add(wid)

    missing = []
    for pair in pair_requests:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        v1, v2 = pair
        if not (v_to_wids.get(v1, set()) & v_to_wids.get(v2, set())):
            missing.append((v1, v2))

    if not missing:
        log_print("\nAll pairing requests were satisfied.")
        return

    log_print(f"\nUnmet pairing requests: {len(missing)}")
    for v1, v2 in missing:
        log_print(f"  {v1} + {v2}")