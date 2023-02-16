"""Functions for calculating the the early hit ranking using Schulze ranking.

See: https://arxiv.org/ftp/arxiv/papers/1804/1804.02973.pdf
See: https://en.wikipedia.org/wiki/Schulze_method
"""
import numpy as np
import pandas as pd
from itertools import permutations


def calculate_combined_rank(per_variant_ranks: pd.DataFrame) -> pd.Series:
    """Calculates a single rank for how early a location is typically hit.

    Combines the ranks of how early a location was hit by each new variant
    into a single score for how early the location gets hit in general by
    applying Schulze voting.

    Args:
         per_variant_ranks: a pandas dataframe in which each column is a
            location,  each row is a variant, and the value is the order
            in which the location was hit by that variant in comparison
            to the other locations.
    Returns:
        a pandas dataframe mapping each location to it's rank.
        Lower values indicate being hit earlier

    """

    preferences = _calculate_pairwise_preferences(per_variant_ranks)
    path_strengths = _calculate_path_strengh(preferences)
    num_wins = _calculate_final_ranking(path_strengths)

    # Flip the num_wins so that 0 is earliest, and higher number is later
    num_wins = abs(num_wins - max(num_wins))

    ranking = pd.Series(num_wins, index=per_variant_ranks.columns)
    ranking = ranking.rank(method='min')

    return ranking


def _calculate_pairwise_preferences(per_variant_ranks: pd.DataFrame) -> np.array:
    """Run a 'who got hit quickest' pairwise voting tournament.

    A variant will 'vote' for a location if it hit said location before the other
    location in the pair, or if said other location wasn't hit at all.

    Args:
        per_variant_ranks: a pandas dataframe in which each column is a
            location,  each row is a variant, and the value is the order
            in which the location was hit by that variant in comparison
            to the other locations.

    Returns:
        a square numpy array (num_locations x num_locations) with the voting totals.
    """
    locations = per_variant_ranks.columns
    num_locations = len(locations)

    preferences = np.zeros((num_locations, num_locations))

    for location_a, location_b in permutations(range(0, num_locations), 2):
        preferences[location_a][location_b] = sum(
            # the number of times location_a was hit before location_b
            per_variant_ranks.iloc[:, location_a] < per_variant_ranks.iloc[:, location_b]
        ) + sum(
            # the number of times location_a was hit and location_b was not hit at all
            ~per_variant_ranks.iloc[:, location_a].isna() & per_variant_ranks.iloc[:, location_b].isna()
        )

    return preferences


def _calculate_path_strengh(preferences: np.array) -> np.array:
    num_locations = preferences.shape[0]

    path_strength = np.zeros(preferences.shape)

    for location_a, location_b in permutations(range(0, num_locations), 2):
        if preferences[location_a, location_b] > preferences[location_b, location_a]:
            path_strength[location_a, location_b] = preferences[location_a, location_b]

    for loc_a, loc_b, loc_c in permutations(range(0, num_locations), 3):
        path_strength[loc_b, loc_c] = max(
            path_strength[loc_b, loc_c],
            min(path_strength[loc_b, loc_a], path_strength[loc_a, loc_c])
        )

    return path_strength


def _calculate_final_ranking(path_strength: np.array) -> np.array:

    num_locations = path_strength.shape[0]
    num_wins = np.zeros(num_locations)

    for loc_a, loc_b in permutations(range(0, num_locations), 2):
        if path_strength[loc_a][loc_b] > path_strength[loc_b][loc_a]:
            num_wins[loc_a] += 1

    return num_wins
