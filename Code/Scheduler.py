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


    # main vws variable
    x = {}
    for v in volunteer:
        for d in date:
            for b in base:
                for r in role:
                    if(availability.get(v, {}).get(d, False) 
                        and base_eligibility(v, b, qual)):
                        x[v, d, b, r] = model.NewBoolVar(f"x[{v},{d},{b},{r}]")

    # helitack variable
    x_heli = {}
    for v in volunteer: 
        if qual.get(v, {}).get("is_helitack", False):
            for d in date:
                if availability.get(v, {}).get(d, False):
                    x_heli[v, d] = model.NewBoolVar(f"heli[{v},{d}]")


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


    # soft penalties
    # minimise penalties

    solver.Solve(model)

    return model

def base_eligibility(v, b, qual):
    # Check if volunteer v is eligible for base b 
    prefs = qual.get(v, {}).get("preferred_bases", [])
    duals = qual.get(v, {}).get("dual_bases", [])
    return (b in prefs) or (b in duals)

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
    qual  = data.get("qual", {})

    # Every demand slot must be filled
    # General
    for d in date:
        for b in base:
            for r in role:
                assigned = [
                    x[v, d, b, r]
                    for v in volunteer
                    if (v, d, b, r) in x
                ]
                model.Add(sum(assigned) == demand[d][b][r])

    # Helitack
    helitack_demand = data["helitack_demand"]
    for d in date:
        required = helitack_demand.get(d, 0)
        if required == 0:
            continue

        assigned_heli = [
            x_heli[v, d]
            for v in volunteer
            if (v, d) in x_heli
        ]
        model.Add(sum(assigned_heli) == required)

    # Control
    for w in data["ctrl_week"]:
        assigned_ctrl = [
            x_ctrl[v, w]
            for v in data["ctrl_qual"]
            if (v, w) in x_ctrl
        ]
    model.Add(sum(assigned_ctrl) == 1)

    # Dispatch
    for d in disp_week:
        for r in disp_role:
            assigned = [
                x_disp[v, d, r]
                for v in disp_volunteer
                if (v, d, r) in x_disp
            ]
            model.Add(sum(assigned) == 1)
    

    # Ensuring no cross-conflict
    weeks_to_weekends = data["weeks_to_weekends"]
    for v in volunteer:
        if v not in ctrl_qual:
            continue

        for w in ctrl_week:
            if (v, w) not in x_ctrl:
                continue

            overlapping_dates = weeks_to_weekends[w]
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