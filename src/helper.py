"""
Helper functions for processing credit card transactions, location validation,
credit score adjustments, and credit limit calculations.

This module contains utility functions that you can modify and extend as needed
for your project. Feel free to update these functions or add new ones while
maintaining the core functionality.
"""


def is_location_close_enough(zip1, zip2):
    """Determine if merchant location is close enough to the customer's address.

    Returns:
        bool: True if the locations are close enough to approve, False if too far apart
    """
    if not zip1 or not zip2 or len(zip1) < 5 or len(zip2) < 5:
        # Can't determine distance with invalid zips
        return False  # Safer to reject when we can't verify

    # Check first digits of zip codes to determine proximity
    # Increased allowed distance by allowing first digit to be different
    if zip1[:1] != zip2[:1]:
        # Different first digit - very far apart (different regions)
        return False

    if zip1[:2] != zip2[:2]:
        # Different second digit but same first digit
        # Moderately far but will now approve these
        return True

    # Same first 2+ digits - close enough
    return True


def calculate_credit_score_adjustment(usage_percentage):
    """
    Calculate credit score adjustment based on credit usage percentage.

    Args:
        usage_percentage: Credit usage as a percentage of total available credit (0-100)

    Returns:
        int: Credit score adjustment (positive or negative)
    """
    # Credit utilization best practices suggest keeping usage below 30%
    if usage_percentage <= 10:
        # Excellent utilization: significant score improvement
        return 15
    elif usage_percentage <= 20:
        # Very good utilization
        return 10
    elif usage_percentage <= 30:
        # Good utilization
        return 5
    elif usage_percentage <= 50:
        # Fair utilization: small penalty
        return -5
    elif usage_percentage <= 70:
        # High utilization: moderate penalty
        return -15
    else:
        # Very high utilization: significant penalty
        return -25


def calculate_new_credit_limit(old_limit, credit_score_change):
    """
    Calculate new credit limit based on credit score changes.

    Args:
        old_limit: Current credit limit
        credit_score_change: Amount the credit score changed

    Returns:
        float: New credit limit
    """
    # Only reduce limits when scores drop
    if credit_score_change >= 0:
        return old_limit

    # Calculate percentage reduction based on score drop
    if credit_score_change <= -20:
        # Significant drop: reduce by 15%
        reduction_factor = 0.85
    elif credit_score_change <= -10:
        # Moderate drop: reduce by 10%
        reduction_factor = 0.90
    else:
        # Small drop: reduce by 5%
        reduction_factor = 0.95

    return round(old_limit * reduction_factor, -2)  # Round to nearest 100
