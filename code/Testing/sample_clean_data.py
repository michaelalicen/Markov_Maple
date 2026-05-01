import json

def get_sample_data():
    """Return a small synthetic dataset matching DataCleaning.py specification.
    Structure follows the comments in DataCleaning.py (bases -> dates -> roles for demand).
    """
    data = {}

    # basic sets
    data["volunteer"] = ["v1", "v2", "v3", "v4"]
    data["date"] = ["2025-11-15", "2025-11-16"]
    data["base"] = ["HDB", "SPS"]
    data["role"] = ["FF", "CL", "ACL", "helitack"]

    # availability
    data["availability"] = {
        "v1": {"2025-11-15": True,  "2025-11-16": False},
        "v2": {"2025-11-15": True,  "2025-11-16": True},
        "v3": {"2025-11-15": True,  "2025-11-16": True},
        "v4": {"2025-11-15": True,  "2025-11-16": True},
    }

    # qualifications and preferences
    data["qual"] = {
        "v1": {
            "role": ["FF"],
            "trainee_roles": [],
            "home_base": "HDB",
            "dual_bases": [],
            "preferred_bases": ["HDB"],
            "extra_shifts": 1,
            "extra_shift_role": "FF",
            "is_helitack": False,
        },
        "v2": {
            "role": ["CL"],
            "trainee_roles": [],
            "home_base": "SPS",
            "dual_bases": [],
            "preferred_bases": ["SPS"],
            "extra_shifts": 0,
            "extra_shift_role": "CL",
            "is_helitack": False,
        },
        "v3": {
            "role": ["ACL", "FF"],
            "trainee_roles": [],
            "home_base": "HDB",
            "dual_bases": ["SPS"],
            "preferred_bases": ["HDB"],
            "extra_shifts": 2,
            "extra_shift_role": "ACL",
            "is_helitack": True,
        },
        "v4": {
            "role": ["FF"],
            "trainee_roles": [],
            "home_base": "SPS",
            "dual_bases": [],
            "preferred_bases": ["SPS"],
            "extra_shifts": 0,
            "extra_shift_role": "FF",
            "is_helitack": True,
        },
    }

    # demand: base -> date -> role -> count (matches DataCleaning.py example)
    data["demand"] = {
        "HDB": {
            "2025-11-15": {"FF": 1, "CL": 0, "ACL": 0, "helitack": 0},
            "2025-11-16": {"FF": 0, "CL": 1, "ACL": 0, "helitack": 0},
        },
        "SPS": {
            "2025-11-15": {"FF": 0, "CL": 1, "ACL": 0, "helitack": 0},
            "2025-11-16": {"FF": 1, "CL": 0, "ACL": 0, "helitack": 0},
        }
    }

    # helitack specific data: volunteer list, weeks (dates) and per-date role demand
    data["heli_volunteer"] = ["v3", "v4"]
    data["heli_week"] = ["2025-11-15", "2025-11-16"]
    data["heli_qual"] = {
        "v3": {"role": "FF", "station": "HDB"},
        "v4": {"role": "CL", "station": "SPS"},
    }
    # heli_demand uses date -> role -> count
    data["heli_demand"] = {
        "2025-11-15": {"FF": 1, "CL": 0, "ACL": 0},
        "2025-11-16": {"FF": 0, "CL": 1, "ACL": 0},
    }

    # control
    data["ctrl_volunteer"] = ["v1", "v3"]
    data["ctrl_week"] = ["2025-11-15", "2025-11-16"]
    data["ctrl_availability"] = {
        "v1": {"2025-11-15": True, "2025-11-16": False},
        "v3": {"2025-11-15": False, "2025-11-16": True},
    }
    data["ctrl_qual"] = {
        "v1": {"role": "Ctrl", "disp_control": True},
        "v3": {"role": "Ctrl", "disp_control": False},
    }

    # dispatch
    data["disp_volunteer"] = ["v2", "v4"]
    data["disp_week"] = ["2025-11-15", "2025-11-16"]
    data["disp_availability"] = {
        "v2": {"2025-11-15": True, "2025-11-16": True},
        "v4": {"2025-11-15": True, "2025-11-16": False},
    }
    data["disp_role"] = ["mgr", "norm"]
    data["disp_qual"] = {
        "v2": {"role": "Disp", "seniority": "mgr"},
        "v4": {"role": "Disp", "seniority": "norm"},
    }

    # weeks_to_weekends mapping (for ctrl/disp conflict checks)
    data["weeks_to_weekends"] = {"2025-11-15": ["2025-11-15"]}

    # pairing requests example
    data["pairing_requests"] = [("v1", "v3")]

    # weekend_map and base_schedule minimal examples
    data["weekend_map"] = {"2025-11-15": "weekend_01", "2025-11-16": "weekend_01"}
    data["base_schedule"] = {"weekend_01": ["HDB", "SPS"]}

    return data


if __name__ == "__main__":
    sample = get_sample_data()
    # print to stdout
    print(json.dumps(sample, indent=2))
    # also write to a file for easy inspection
    out_path = "sample_clean_data.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(sample, f, indent=2)
    print(f"Wrote sample data to {out_path}")
