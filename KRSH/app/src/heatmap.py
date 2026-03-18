import pandas as pd

def pattern_heatmap(patterns):

    counts = {
        "RBR":0,
        "DBR":0,
        "RBD":0,
        "DBD":0
    }

    for p in patterns:
        counts[p[0]] += 1

    df = pd.DataFrame.from_dict(counts,orient="index",columns=["count"])

    return df