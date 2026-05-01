from sample_clean_data import get_sample_data
from Scheduler import build_and_solve

res = build_and_solve( get_sample_data() )

if isinstance(res, tuple):
    solver, status, x, x_heli, x_ctrl, x_disp, model = res
    print("Status:", status)
    # print some assignments if solver is present
    for key, var in x.items():
        if solver.Value(var):
            print("VWS:", key)
    # heli assignments with base lookup
    for key, var in x_heli.items():
        if solver.Value(var):
            v, d, r = key
            bases_assigned = [
                b for (vv, dd, b, rr), v2 in x.items()
                if vv == v and dd == d and rr == r and solver.Value(v2)
            ]
            if bases_assigned:
                print("Heli:", key, "base(s):", bases_assigned)
            else:
                print("Heli:", key, "base(s): none (heli-only assignment)")
    # print control assignments
    for key, var in x_ctrl.items():
        if solver.Value(var):
            print("Control:", key)
    # print dispatch assignments
    for key, var in x_disp.items():
        if solver.Value(var):
            print("Dispatch:", key)
else:
    # build_and_solve returned model only
    print("build_and_solve returned:", type(res))
    # If you need assignments, update Scheduler.build_and_solve to return solver and var maps.