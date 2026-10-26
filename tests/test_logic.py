def test_esg_risk_logic():
    """Testing how converts sentiment into risk_score (logic from processor)"""
    model_score = 0.85
    
    #If model says NEGATIVE, risk should be high
    label_neg = "NEGATIVE"
    risk_neg = model_score if label_neg == 'NEGATIVE' else (1.0 - model_score)
    assert risk_neg == 0.85, "Risk for NEGATIVE is calculated wrong!"
    
    # If model says POSITIVE, risk should be low
    label_pos