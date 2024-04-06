import pandas as pd


def add_extra_rows(df):
    """
    Function to add extra rows to a DataFrame (df) with specified data,
    representing additional reviews for testing purposes.
    """
    rows = [
        {
            'overall': 7.0, 
            'verified': True, 
            'reviewTime': '09 3, 2015', 
            'asin': 'B000KPIHQ4',
            'reviewText': 'Added for testing tfdv', 
            'summary': 'Good', 
            'Product_Type': 'AMAZON_FASHION_5'
        },
        {
            'overall': 5.0, 
            'verified': True, 
            'reviewTime': '09 7, 2017', 
            'asin': 'B000KPIHQ4',
            'reviewText': 'Added for testing tfdv', 
            'summary': 'Good', 
            'Product_Type': 'AMAZON_FASHION_6'
        },
        {
            'overall': 5.0, 
            'verified': True, 
            'reviewTime': '09 3, 2022', 
            'asin': 'B000KPIHQ4',
            'reviewText': 'Added for testing tfdv', 
            'summary': 'Bad', 
            'Product_Type': 'AMAZON_FASHION_6'
        }
    ]
    # Convert list of dictionaries to DataFrame before concatenation
    rows_df = pd.DataFrame(rows)
    
    # Use pd.concat() instead of .append()
    df = pd.concat([df, rows_df], ignore_index=True) 
    return df