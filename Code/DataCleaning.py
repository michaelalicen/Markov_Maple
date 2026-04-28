'''
    How the data should look:
    data = {
    --- general ---
    "volunteer": ["v001", "v002", ...],  # list of volunteer IDs
    
    "date": ["2025-11-15", "2025-11-16", ...],  # all standby dates in season
    
    "base": ["HDB", "STB", "SPS", "NWL"],
    
    "role": ["FF", "recruit_FF", "ACL", "trainee_ACL", "CL", 
              "crew_driver", "skid_driver", "logistics", "helitack",
              "control", "dispatch"],
    
    "availability": {
        "v001": {
            "2025-11-15": True,
            "2025-11-16": False,
            ...
        },
        ...
    },
    
    "qual": {
        "v001": {
            "role": ["CL", "ACL", "FF"],        # fully qualified roles
            "trainee_roles": [],                   # roles still in training
            "home_base": "HDB",
            "dual_bases": ["NWL"],                 # bases they'll cover as dual
            "preferred_bases": ["HDB"],
            "extra_shifts": 2,                     # how many extras willing to do
            "extra_shift_role": "CL",              # preferred role for extras
            "is_helitack": False,
            "helitack_role": None                  # e.g. "spotter", "rappeller"
        },
        ...
    },
    
    "demand": {
        "HDB": {
            "2025-11-15": {
                "FF": 4,
                "CL": 1,
                "ACL": 1,
                "skid_driver": 1,
                "crew_driver": 1,
                "logistics": 1,
                ...
            },
            ...
        },
        ...
    },
    
    "pairing_requests": [
        ("v001", "v002"),  # these two have requested to be paired
        ...
    ],
    
    "weekend_map": {
        "2025-11-15": "weekend_01",  # map individual dates to their weekend
        "2025-11-16": "weekend_01",
        "2025-11-22": "weekend_02",
        ...
    },

    "weeks_to_weekends": {
        # maps the ctrl/disp weeks to the weekends so that there is no overlap
        "2025-11-26": ["2025-11-29", "2025-11-30"], 
        "2025-12-03": ["2025-12-06", "2025-12-07"],
    },

    "base_schedule": {
        "weekend_01": ["HDB", "SPS"],  # which bases are active this weekend
        "weekend_02": ["STB", "SPS"],  # HDB/STB alternate
        ...
    }

    #--- control ---
    "ctrl_volunteer": [<volunteer_id_ctrl>],
    "ctrl_weeks": [...],
    "ctrl_availability": {...},
    "ctrl_quals": {
        "v001": {
            "role": "Ctrl",
            "disp_control": True # whether this volunteer can be dispatched to control and dispatch simultaneously
        },
        ...
    },

    # --- dispatch ---
    "disp_volunteer": [<volunteer_id_disp>],
    "disp_weeks": [...],
    "disp_availability": {...},
    "disp_role": ["mgr", "norm", "trainee"],
    "disp_quals": {
        "v001": {
            "role": "Disp",
            "seniority": "mgr",

        },
        ...
    },

}

'''

def load_and_validate(data):
    # Validate and clean the input data
    # Return structured data for availability and qualifications
    pass