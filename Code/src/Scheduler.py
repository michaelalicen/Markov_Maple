from ortools.sat.python import cp_model

volunteer = None
availability = None
qual = None
date = None
base = None
role = None
demand  = None
ctrl_volunteer = None
ctrl_availability = None
ctrl_qual = None
ctrl_week = None
disp_volunteer = None
disp_availability = None
disp_qual = None
disp_week = None
disp_role = None
heli_volunteer = None
heli_qual = None
heli_week = None
weeks_to_weekends = None

def build_and_solve(data):
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
    primary = qual.get(v, {}).get("home_base")
    secondary = qual.get(v, {}).get("secondary_bases") 
    tertiary = qual.get(v, {}).get("tertiary_base")
    # If volunteer has no base preference information, allow assignment by default

    return (b == primary) or (b == secondary) or (b == tertiary)

def hard_constraints(model, x, x_ctrl, x_disp, x_heli):
    demand_general(model, x)
    one_shift_per_weekend(model, x)
    demand_heli(model, x_heli)
    demand_contr_disp(model, x_ctrl, x_disp)
    no_overlap(model, x, x_ctrl, x_disp)
    trainee_with_senior(model, x)

def demand_general(model):
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

def demand_heli(model):
    # Demand slots for Heli-tack must be filled
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
            # ensure heli demand is added to the model
            model.Add(sum(assigned) == required)

def demand_contr_disp(model, x_ctrl, x_disp):
    # Control: exactly one control per ctrl_week
    for w in data.get("ctrl_week", []):
        assigned_ctrl = [
            x_ctrl[v, w]
            for v in data.get("ctrl_volunteer", [])
            if (v, w) in x_ctrl
        ]
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
            if str(r).lower() == "trainee":
                model.Add(sum(assigned) <= 1)
            else:
                model.Add(sum(assigned) == 1)

def no_overlap(model, x, x_ctrl, x_disp):
    # Ensuring no cross-conflict for ctrl
    for v in ctr_volunteer:
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

    # ensures no cross conflict for dispatch
    for v in disp_volunteer:
        if v not in data.get("disp_qual", {}):
            continue

        for w in data.get("disp_week", []):
            if (v, w) not in x_disp:
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

def trainee_with_senior(model, x):
    # Ensure at least one senior is assigned if a trainee is assigned
    for (v, d, b, r), v_var in list(x.items()):
        trainee_roles = qual.get(v, {}).get('trainee_roles', []) or []
        trainee_roles = [str(tr) for tr in trainee_roles]
        if str(r) not in trainee_roles:
            continue

        # collect senior candidate variables on same date/base in any other role
        senior_vars = []
        for u in volunteer:
            if u == v:
                continue
            for r2 in role:
                if str(r2) == str(r):
                    # senior must be in a different role than the trainee
                    continue
                if (u, d, b, r2) not in x:
                    continue
                u_roles = qual.get(u, {}).get('role', []) or []
                u_trainee_roles = qual.get(u, {}).get('trainee_roles', []) or []
                u_roles = [str(rr) for rr in u_roles]
                u_trainee_roles = [str(tr) for tr in u_trainee_roles]
                # senior must have the role and must not be a trainee for it
                if str(r2) in u_roles and str(r2) not in u_trainee_roles:
                    senior_vars.append(x[u, d, b, r2])

        model.Add(sum(senior_vars) >= v_var)

# Hard constraint for general, soft constraint for helitack
def one_shift_per_weekend(model, x):
    """
    Enforce that each volunteer has at most one general VWS assignment per weekend.
    Uses the weeks_to_weekends mapping (week_key -> list of date strings).
    This only considers the general VWS variables in x (not heli, control, dispatch).
    """
    # weeks_to_weekends is expected to be a mapping from a week id to a list of dates
    for v in volunteer:
        for wk, dates in weeks_to_weekends.items():
            # collect assignment variables for this volunteer across all dates in the weekend
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
    # Define the soft constraints here
    # model.Add(...)
    pass