import logging
import math
from typing import List, Tuple

import numpy as np
from scipy import stats

logger = logging.getLogger(__name__)

def update_skill_score(
    prior_mean: float, 
    prior_var: float, 
    new_clv_observations: List[float]
) -> Tuple[float, float]:
    """
    Perform a Bayesian update on the trader's skill (assumed to be represented by CLV).
    Skill ~ Normal(prior_mean, prior_var)
    Data ~ Normal(skill, sigma_noise)
    """
    if not new_clv_observations:
        return prior_mean, prior_var
        
    sigma_noise = 0.1 # Constant assumption about observation noise
    n = len(new_clv_observations)
    data_mean = np.mean(new_clv_observations)
    
    # Posterior Precision = Prior Precision + Data Precision
    precision_prior = 1.0 / prior_var
    precision_data = n / (sigma_noise ** 2)
    precision_post = precision_prior + precision_data
    
    # Posterior Mean = (Prior Precision * Prior Mean + Data Precision * Data Mean) / Posterior Precision
    mean_post = (precision_prior * prior_mean + precision_data * data_mean) / precision_post
    var_post = 1.0 / precision_post
    
    return mean_post, var_post

def apply_shrinkage(score: float, trades_count: int, threshold: int = 10) -> float:
    """
    Apply partial pooling (shrinkage) toward zero for low trade counts.
    If count < threshold, the score is significantly reduced.
    """
    if trades_count >= threshold:
        return score
    
    shrinkage_factor = trades_count / threshold
    return score * shrinkage_factor

def compute_composite_skill(repricing_skill: float, resolution_skill: float) -> float:
    """
    Combine near-term predictive skill (repricing/CLV) 
    with long-term predictive skill (realized edge/PnL).
    """
    return (0.6 * repricing_skill) + (0.4 * resolution_skill)
