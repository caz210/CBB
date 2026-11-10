import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def make_league(n=40, seed=7):
    """Synthetic KenPom-shaped league using real API column names."""
    rng = np.random.default_rng(seed)
    names = [f"Team{i:02d}" for i in range(n)]
    oe = rng.normal(106, 7, n)
    de = rng.normal(106, 7, n)
    em = oe - de
    rank = (-em).argsort().argsort() + 1
    ratings = pd.DataFrame({"TeamName": names, "AdjOE": oe, "AdjDE": de, "AdjEM": em,
                            "RankAdjEM": rank, "AdjTempo": rng.normal(68, 3, n),
                            "APL_Off": rng.normal(17.5, 1.2, n), "APL_Def": rng.normal(17.5, 1.2, n)})
    ff = pd.DataFrame({"TeamName": names, "eFG_Pct": rng.normal(51, 3, n), "TO_Pct": rng.normal(17, 2, n),
                       "OR_Pct": rng.normal(29, 4, n), "FT_Rate": rng.normal(33, 5, n),
                       "DeFG_Pct": rng.normal(51, 3, n), "DTO_Pct": rng.normal(17, 2, n),
                       "DOR_Pct": rng.normal(29, 4, n), "DFT_Rate": rng.normal(33, 5, n)})
    height = pd.DataFrame({"TeamName": names, "AvgHgt": rng.normal(77.5, 1, n),
                           "Exp": rng.normal(1.8, 0.4, n), "Bench": rng.normal(31, 5, n)})
    misc = pd.DataFrame({"TeamName": names, "FTPct": rng.normal(71, 4, n),
                         "F3GRate": rng.normal(38, 5, n), "OppF3GRate": rng.normal(38, 5, n),
                         "FG3Pct": rng.normal(34, 3, n), "OppFG3Pct": rng.normal(34, 3, n)})
    return {"ratings": ratings, "four_factors": ff, "height": height, "misc": misc,
            "net": pd.DataFrame(columns=["TeamName", "Rank"]),
            "hca": pd.DataFrame({"TeamName": ["Team00", "Team01"], "HCA": [5.0, 2.0]})}


@pytest.fixture
def league():
    return make_league()
