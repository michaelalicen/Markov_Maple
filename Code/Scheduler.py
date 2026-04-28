from ortools.sat.python import cp_model

def build_and_solve(data):
    model = cp_model.CpModel()
    
    volunteer = data["volunteer"]
    availability = data["availability"]
    qual = data["qual"]
    date = data["date"]
    base = data["base"]
    role = data["role"]

    ctrl_volunteer = data["ctrl_volunteer"]
    ctrl_availability = data["ctrl_availability"]
    ctrl_quals = data["ctrl_quals"]
    ctrl_weeks = data["ctrl_weeks"]

    disp_volunteer = data["disp_volunteer"]
    disp_availability = data["disp_availability"]
    disp_quals = data["disp_quals"]
    disp_weeks = data["disp_weeks"]
    disp_role = data["disp_role"]

    # main vws variable
    # creates an empty dictionary
    x = {}
    for v in volunteer:
        for d in date:
            for b in base:
                for r in role:
                    if(availability.get(v, {}).get(d, False) 
                        and base_eligibility(v, b, qual) 
                        and is_active(v, qual)):
                        x[v, d, b, r] = model.NewBoolVar(f"x[{v},{d},{b},{r}]")

    # helitack variable
    x_heli = {}
    for v in [v for v in volunteer if qual[v]["is_helitack"]]:
        for d in date:
            if availability.get(v, {}).get(d, False):
                x_heli[v, d] = model.NewBoolVar(f"heli[{v},{d}]")


    # control variable
    x_ctrl = {}
    for v in [v for v in ctrl_volunteer if ctrl_quals[v]["active"]]:
        for d in ctrl_weeks:
            if ctrl_availability.get(v, {}).get(d, False):
                x_ctrl[v, d] = model.NewBoolVar(f"ctrl[{v},{d}]")

    # dispatch variable
    x_disp = {}
    for v in [v for v in disp_volunteer if disp_quals[v]["active"]]:
        for d in disp_weeks:
            for r in disp_role:
                if disp_availability.get(v, {}).get(d, False):
                    x_disp[v, d, r] = model.NewBoolVar(f"disp[{v},{d},{r}]")

    # hard constraints

    # soft penalties
    # minimise penalties

    # solver.Solve(model)

    return model

def base_eligibility(v, b, qual):
    # Check if volunteer v is eligible for base b
    if b in qual[v]["preferred_bases"]:
        return True
    elif b in qual[v]["dual_bases"]:
        return True
    else:
        return False

'''
    Coverage: sum(x[v,d,b,r] for v) == demand[d][b][r] — every required slot must be filled
    Availability: x[v,d,b,r] = 0 if volunteer not available on that day
    Qualification: x[v,d,b,r] = 0 if volunteer not qualified for that role (handled by pre-filter)
    Once per weekend: sum(x[v,d,b,r] for all b,r on weekend w) <= 1
    Trainee pairing: if trainee driver/ACL assigned, at least one senior in that role on same crew
    Helitack submodel: Helitack pool and demand solved separately; ~7 CLs for 26 dates is tight

'''
def hard_constraints(model):
    date  = data["date"]
    base  = data["base"]
    role  = data["role"]
    demand  = data["demand"]
    qual  = data["qual"]



    # Every demand slot must be filled
    # General
    for d in dates:
        for b in bases:
            for r in roles:
                assigned = [
                    x[v, d, b, r]
                    for v in volunteer
                    if (v, d, b, r) in x
                ]
                model.Add(sum(assigned) == demand[d][b][r])

    # Helitack
    helitack_demand = data["helitack_demand"]
    for d in dates:
        required = helitack_demand.get(d, 0)
        if required == 0:
            continue

        assigned_heli = [
            x_heli[v, d]
            for v in volunteers
            if (v, d) in x_heli
        ]
        model.Add(sum(assigned_heli) == required)

    # Control
    for w in data["ctrl_weeks"]:
        assigned_ctrl = [
            x_ctrl[v, w]
            for v in data["ctrl_quals"]
            if (v, w) in x_ctrl
        ]
    model.Add(sum(assigned_ctrl) == 1)

    # Dispatch
    

    # Ensuring no cross-conflict
    weeks_to_weekends = data["weeks_to_weekends"]
    for v in volunteers:
        if v not in ctrl_quals:
            continue

        for w in ctrl_weeks:
            if (v, w) not in x_ctrl:
                continue

            overlapping_dates = weeks_to_weekends[w]
            vws_assignments = [
                x[v, d, b, r]
                for d in overlapping_dates
                for b in bases
                for r in roles
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
    model.Add(...)

def solver(model):
    solver = cp_model.CpSolver()
    status = solver.Solve(model)
    if status == cp_model.OPTIMAL:
        print("Solution found!")
    else:
        print("No solution found.")
