from Scheduler import build_and_solve

def make_tiny_dataset():
    # Tiny synthetic dataset that conforms to expected structure
    data = {}

    # basic sets
    data["volunteer"] = ["v1", "v2", "v3"]
    data["date"] = ["2025-11-15"]
    data["base"] = ["HDB", "SPS"]
    data["role"] = ["FF", "CL"]

    # availability: everyone available that date
    data["availability"] = {
        "v1": {"2025-11-15": True},
        "v2": {"2025-11-15": True},
        "v3": {"2025-11-15": True},
    }

    # qualifications and preferences
    data["qual"] = {
        "v1": {"role": ["FF"], "preferred_bases": ["HDB"], "dual_bases": [], "is_helitack": False},
        "v2": {"role": ["CL"], "preferred_bases": ["SPS"], "dual_bases": [], "is_helitack": False},
        "v3": {"role": [], "preferred_bases": ["HDB"], "dual_bases": [], "is_helitack": False},
    }

    # demand: one FF at HDB and one CL at SPS on that date
    data["demand"] = {
        "2025-11-15": {
            "HDB": {"FF": 1, "CL": 0},
            "SPS": {"FF": 0, "CL": 1},
        }
    }

    # control setup: v3 is a control candidate for that week/date (not v1)
    data["ctrl_volunteer"] = ["v3"]
    data["ctrl_week"] = ["2025-11-15"]
    data["ctrl_availability"] = {"v3": {"2025-11-15": True}}
    data["ctrl_qual"] = {"v3": {"active": True}}

    # dispatch setup: require one mgr and one norm for that week; assign v2->mgr, v3->norm available
    data["disp_volunteer"] = ["v2", "v3"]
    data["disp_week"] = ["2025-11-15"]
    data["disp_role"] = ["mgr", "norm"]
    data["disp_availability"] = {
        "v2": {"2025-11-15": True},
        "v3": {"2025-11-15": True},
    }
    data["disp_qual"] = {
        "v2": {"active": True},
        "v3": {"active": True},
    }

    # mapping weeks to overlapping weekend dates (for cross-conflict)
    data["weeks_to_weekends"] = {
        "2025-11-15": ["2025-11-15"]
    }

    return data

def print_solution(solver, status, x, x_heli, x_ctrl, x_disp):
    if status not in (0, 4):  # 4=OPTIMAL, 0=UNKNOWN/FEASIBLE depending on OR-Tools; check status constants if needed
        print("No solution found.")
        return
    print("Assigned VWS slots:")
    for key, var in list(x.items()):
        if solver.Value(var):
            v, d, b, r = key
            print(f"  {d} {b} {r} <- {v}")

    print("\nAssigned control:")
    for key, var in list(x_ctrl.items()):
        if solver.Value(var):
            v, w = key
            print(f"  control {w} <- {v}")

    print("\nAssigned dispatch:")
    for key, var in list(x_disp.items()):
        if solver.Value(var):
            v, w, r = key
            print(f"  dispatch {w} role {r} <- {v}")

if __name__ == "__main__":
    data = make_tiny_dataset()
    solver, status, x, x_heli, x_ctrl, x_disp, model = build_and_solve(data)
    print_solution(solver, status, x, x_heli, x_ctrl, x_disp)