from typing import Dict, Any, List

def generate_signal_explanation(signal_strength: float, bias: str, contributors: List[Dict[str, Any]]) -> str:
    """
    Generate a human-readable explanation for a market signal.
    """
    if not contributors:
        return "Low-conviction signal based on general market activity."
        
    num_skilled = len(contributors)
    top_label = contributors[0]["label"] if contributors else "traders"
    
    narrative = f"Strong {bias} signal ({signal_strength:.4f}) detected. "
    narrative += f"Driven by {num_skilled} skilled analysts. "
    narrative += f"Lead conviction from a recognized {top_label}."
    
    return narrative

def format_trader_skill(address: str, label: str, skill_score: float, top_tags: List[str]) -> str:
    """Format a trader's skill profile into a narrative summary."""
    tag_str = ", ".join(top_tags[:2])
    return f"Trader {address[:8]}... is a {label} specialist in {tag_str} with a skill score of {skill_score:.4f}."
