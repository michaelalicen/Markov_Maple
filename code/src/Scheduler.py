from ortools.sat.python import cp_model
import re
import time

# Maps the short disp_role keys in the JSON to the full names used in disp_demand
DISP_ROLE_TO_DEMAND_KEY = {
    "mgr": "Dispatch Manager",
    "norm": "Dispatcher",
    "trainee": "Trainee Dispatcher",
}

def build_and_solve(data):
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

    # helitack variable: keyed by (v, d, role)
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

    # solver
    solver = cp_model.CpSolver()
    start = time.time()
    status = solver.Solve(model)
    end = time.time()
    elapsed = end - start
 
    if status == cp_model.OPTIMAL:
        print(f"Solver status: OPTIMAL (time: {elapsed:.2f}s)")
    elif status == cp_model.FEASIBLE:
        print(f"Solver status: FEASIBLE (time: {elapsed:.2f}s)")
    elif status == cp_model.INFEASIBLE:
        print(f"Solver status: INFEASIBLE (time: {elapsed:.2f}s)")
        diagnose_infeasibility(data, x, x_heli, x_ctrl, x_disp)
    else:
        print(f"Solver status: {solver.StatusName(status)} (time: {elapsed:.2f}s)")

    return solver, status, x, x_heli, x_ctrl, x_disp, model

def base_eligibility(v, b, qual):
    """Check if volunteer v is eligible for base b.

    This normalises common base name variants (e.g. "NWL HC1" -> "NWL_HC1",
    "HDB HC/SU" -> "HDB_HC") and accepts primary/secondary/tertiary values
    that may be single strings or lists.
    """
    def _norm(name):
        if name is None:
            return None
        s = str(name).strip().upper()
        sub = {
            "NWL HC1": "NWL_HC1", "NWL HC2": "NWL_HC2", "NWL SU": "NWL_SU",
            "SPS BP": "SPS_BP", "SPS HC": "SPS_HC", "HDB HC/SU": "HDB_HC",
            "STB HC": "STB_HC", "HELITACK STB": "HELITACK_STB",
        }
        if s in sub:
            return sub[s]

    PRIMARY_TO_SUBCREWS = {
        "HDB": ["HDB_HC"],
        "STB": ["STB_HC"],
        "NWL": ["NWL_HC1", "NWL_HC2", "NWL_SU"],
        "SPS": ["SPS_HC", "SPS_BP"],
    }

    v_qual = qual.get(v, {}) if isinstance(qual, dict) else {}
    entries = []
    primary = v_qual.get("home_base")
    secondary = v_qual.get("secondary_base") or v_qual.get("secondary_bases")
    tertiary = v_qual.get("tertiary_base")

    for e in (primary, secondary, tertiary):
        if e is None:
            continue
        if isinstance(e, (list, tuple, set)):
            entries.extend(list(e))
        else:
            entries.append(e)

    allowed = set()
    for e in entries:
        norm = _norm(e)
        if norm:
            allowed.add(norm)
        # expand short primary codes to their subcrew variants
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

            # variables directly compatible with the demanded heli_role
            assigned = [
                var for (v_key, d_key, r_key), var in x_heli.items()
                if d_key == d and _compatible(r_key, heli_role)
            ]

            model.Add(sum(assigned) == required)

def demand_contr_disp(model, data, x_ctrl, x_disp):
    """Control and dispatch demand constraints.

    FIX: Read required counts from ctrl_demand and disp_demand in the data
    instead of hardcoding 1. Also map disp_role short keys to demand key names.
    """
    ctrl_demand = data.get("ctrl_demand", {})

    # Control: fill demand from ctrl_demand[week]["Control"]
    for w in data.get("ctrl_week", []):
        required = int(ctrl_demand.get(w, {}).get("Control") or 0)
        assigned_ctrl = [
            x_ctrl[v, w]
            for v in data.get("ctrl_volunteer", [])
            if (v, w) in x_ctrl
        ]
        model.Add(sum(assigned_ctrl) == required)

    disp_demand = data.get("disp_demand", {})

    # Dispatch: fill demand from disp_demand[week][full_role_name]
    # disp_role uses short keys ("mgr", "norm", "trainee"); disp_demand uses full names
    for d in data.get("disp_week", []):
        for r in data.get("disp_role", []):
            demand_key = DISP_ROLE_TO_DEMAND_KEY.get(r, r)
            required = int(disp_demand.get(d, {}).get(demand_key) or 0)
            assigned = [
                x_disp[v, d, r]
                for v in data.get("disp_volunteer", [])
                if (v, d, r) in x_disp
            ]
            # trainee role can be 0 or up to required
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


'''
    Fairness: Minimise imbalance in total shifts assigned — use max deviation from mean as penalty term
    No back-to-back: Penalise consecutive weekend assignments; do not hard-ban (may be unavoidable)
    Burnout cap: <= 3 shifts over any rolling 2-month window; soft penalty rather than hard cut-off
    Base preferences: Reward primary standby base match; reward extra-shift willingness flag
    Pairing requests: Pairing requests
'''
def soft_constraints(model):
    pass