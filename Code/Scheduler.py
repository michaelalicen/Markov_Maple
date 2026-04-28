from ortools.sat.python import cp_model
'''
    availability[volunteer_id][date] = True/False
    quals[volunteer_id] = {
        "roles": [],
        "trainee_roles": [],
        "active": True/False,
        "preferred_bases": "[base1, base2, base3]",
        "extra_shifts": [0,1,2,3,4,5],
        "extra_shift_role": "role_id"
    }
'''

def build_and_solve(data):
    model = cp_model.CpModel()
    
    volunteer = data["volunteer"]
    availability = data["availability"]
    qual = data["qual"]
    date = data["date"]
    base = data["base"]
    role = data["role"]

    # main vws variable
    # creates an empty dictionary
    x = {}
    for v in volunteer:
        for d in date:
            for b in base:
                for r in role:
                    if(base_eligibility(v, b, qual) and is_active(v, qual)):
                        x[v, d, b, r] = model.NewBoolVar(f"x[{v},{d},{b},{r}]")

    # helitack variable
    x_heli = {}
    

    # control variable

    # dispatch variable

    return model

def base_eligibility(v, b, qual):
    # Check if volunteer v is eligible for base b
    if b in qual[v]["preferred_bases"]:
        return True
    elif b in qual[v]["dual_bases"]:
        return True
    else:
        return False

def is_active(v, qual):
    # Check if volunteer v is active
    return qual[v]["active"]


'''
    Assignment priority
    1. Non-fireline: Logistics & Planning
    2. Drivers: Skids first, then crew
    3. Crew Leaders: Use duals if gaps
    4. ACLs: Trainee ACLs later in season
    5-6. Firefighters: Recruits, then general FFs
'''
def objective_function(model):
    # Define the objective function here
    model.Maximize(...)

'''
    Coverage: sum(x[v,d,b,r] for v) == demand[d][b][r] — every required slot must be filled
    Availability: x[v,d,b,r] = 0 if volunteer not available on that day
    Qualification: x[v,d,b,r] = 0 if volunteer not qualified for that role (handled by pre-filter)
    Once per weekend: sum(x[v,d,b,r] for all b,r on weekend w) <= 1
    Trainee pairing: if trainee driver/ACL assigned, at least one senior in that role on same crew
    Helitack submodel: Helitack pool and demand solved separately; ~7 CLs for 26 dates is tight

'''
def hard_constraints(model):
    # Define the hard constraints here
    model.Add(...)

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
