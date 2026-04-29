from ortools.sat.python import cp_model

def build_and_solve(data):
    model = cp_model.CpModel()
    
    volunteer = data.get("volunteer", [])
    availability = data.get("availability", {})
    qual = data.get("qual", {})
    date = data.get("date", [])
    base = data.get("base", [])
    role = data.get("role", [])

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
    heli_candidates = heli_volunteer if heli_volunteer else list(heli_qual.keys())
    heli_dates = heli_week if heli_week else date
    for v in heli_candidates:
        v_role = heli_qual.get(v, {}).get("role")
        # if volunteer has a recorded heli role, create var for that role only
        if v_role:
            for d in heli_dates:
                if availability.get(v, {}).get(d, False):
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

    hard_constraints(model, data, volunteer, x, x_ctrl, x_disp, x_heli)

    # solver
    solver = cp_model.CpSolver()
    status = solver.Solve(model)

    return solver, status, x, x_heli, x_ctrl, x_disp, model


def base_eligibility(v, b, qual):
    # Check if volunteer v is eligible for base b
    prefs = qual.get(v, {}).get("preferred_bases", []) or []
    duals = qual.get(v, {}).get("dual_bases", []) or []
    home = qual.get(v, {}).get("home_base")
    # If volunteer has no base preference information, allow assignment by default
    if not prefs and not duals and not home:
        return True
    return (b in prefs) or (b in duals) or (home == b)

'''
    Coverage: sum(x[v,d,b,r] for v) == demand[d][b][r] — every required slot must be filled
    Availability: x[v,d,b,r] = 0 if volunteer not available on that day
    Qualification: x[v,d,b,r] = 0 if volunteer not qualified for that role (handled by pre-filter)
    Once per weekend: sum(x[v,d,b,r] for all b,r on weekend w) <= 1
    Trainee pairing: if trainee driver/ACL assigned, at least one senior in that role on same crew
    Helitack submodel: Helitack pool and demand solved separately; ~7 CLs for 26 dates is tight

'''
def hard_constraints(model, data, volunteer, x, x_ctrl, x_disp, x_heli):
    date  = data.get("date", [])
    base  = data.get("base", [])
    role  = data.get("role", [])
    demand  = data.get("demand", {})
    availability = data.get("availability", {})
    qual = data.get("qual", {})
    disp_volunteer = data.get("disp_volunteer", [])
    disp_availability = data.get("disp_availability", {})

    # Every demand slot must be filled (support both demand shapes)
    for d in date:
        for b in base:
            for r in role:
                # try two shapes: demand[date][base][role] or demand[base][date][role]
                required = None
                required = demand.get(d, {}).get(b, {}).get(r) if isinstance(demand, dict) else None
                if required is None:
                    required = demand.get(b, {}).get(d, {}).get(r) if isinstance(demand, dict) else None
                required = int(required or 0)

                assigned = [
                    x[v, d, b, r]
                    for v in volunteer
                    if (v, d, b, r) in x
                ]

                model.Add(sum(assigned) == required)

    # Helitack: meet per-date, per-role heli demand in data["heli_demand"]
    heli_demand = data.get("heli_demand", {})
    heli_dates = data.get("heli_week", date)
    heli_vols = data.get("heli_volunteer", [])
    heli_qual = data.get("heli_qual", {})
    heli_candidates = heli_vols if heli_vols else list(heli_qual.keys())
    for d in heli_dates:
        role_map = heli_demand.get(d, {})
        for heli_role, required in role_map.items():
            required = int(required or 0)
            if required == 0:
                continue
            # collect x_heli variables for this date and role
            assigned = [
                var for (v_key, d_key, r_key), var in x_heli.items()
                if d_key == d and str(r_key) == str(heli_role)
            ]
            if required > 0 and len(assigned) == 0:
                potential = [
                    v for v in heli_candidates
                    if availability.get(v, {}).get(d, False) and str(heli_qual.get(v, {}).get("role")) == str(heli_role)
                ]

    # Control: exactly one control per ctrl_week
    for w in data.get("ctrl_week", []):
        assigned_ctrl = [
            x_ctrl[v, w]
            for v in data.get("ctrl_volunteer", [])
            if (v, w) in x_ctrl
        ]
        # If the week expects a control but we have no available control variables, raise diagnostic
        model.Add(sum(assigned_ctrl) == 1)

    # Dispatch: every week there is one of each dispatch role
    # (except trainee: can be zero or one)
    for d in data.get("disp_week", []):
        for r in data.get("disp_role", []):
            assigned = [
                x_disp[v, d, r]
                for v in data.get("disp_volunteer", [])
                if (v, d, r) in x_disp
            ]
            # treat 'trainee' role as optional (0 or 1)
            if str(r) == "trainee":
                model.Add(sum(assigned) <= 1)
            else:
                model.Add(sum(assigned) == 1)

    # Ensuring no cross-conflict for ctrl
    weeks_to_weekends = data.get("weeks_to_weekends", {})
    for v in volunteer:
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

'''
    Fairness: Minimise imbalance in total shifts assigned — use max deviation from mean as penalty term
    No back-to-back: Penalise consecutive weekend assignments; do not hard-ban (may be unavoidable)
    Burnout cap: <= 3 shifts over any rolling 2-month window; soft penalty rather than hard cut-off
    Base preferences: Reward primary standby base match; reward extra-shift willingness flag
    Pairing requests: Pairing requests

'''
def soft_constraints(model):
    # Define the soft constraints here
    # model.Add(...)
    pass