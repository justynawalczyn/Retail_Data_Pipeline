SELECT * FROM grocery_sales

import pandas as pd
import os

# Extract function is already implemented for you 
def extract(store_data, extra_data):
    extra_df = pd.read_parquet(extra_data)
    merged_df = store_data.merge(extra_df, on = "index")
    return merged_df

# Call the extract() function and store it as the "merged_df" variable
merged_df = extract(grocery_sales, "extra_data.parquet")


missing_info = pd.DataFrame({
    "Missing Values": merged_df.isnull().sum(),
    "Data Type": merged_df.dtypes
})
print(missing_info)

#Mean (Average)	When data is normally distributed (no extreme values)	The mean represents the central tendency well for symmetric distributions.
#Median	When data has outliers or is skewed	The median is more robust to outliers because it’s the middle value, unaffected by extreme numbers.


# the transform() function with one parameter: "raw_data"
#def transform(raw_data):
    

  #  merged_df.fillna(
   #     {
    #       'CPI': raw_data['CPI'].mean(),
    #      'Weekly_Sales': raw_data['Weekly_Sales'].mean(),
     #     'Unemployment': raw_data['Unemployment'].mean(),
     # }, inplace = True
    #)
    
def transform(raw_data):
    # numeric columns
    num_cols = ['Weekly_Sales', 'CPI', 'Unemployment']
    raw_data[num_cols] = raw_data[num_cols].fillna(raw_data[num_cols].mean())

    #
    markdown_cols = ['MarkDown4', 'MarkDown5']
    raw_data[markdown_cols] = raw_data[markdown_cols].fillna(0)

    #  categorical columns -> (mode)
    categorical_cols = ['Type']
    for col in categorical_cols:
        raw_data[col].fillna(raw_data[col].mode()[0], inplace=True)

    # "Date" forward-fill (assumes missing values should take the previous date)
    raw_data['Date'] = raw_data['Date'].fillna(method='ffill')

    # "Size" with the median (as store sizes can vary widely)
    raw_data['Size'] = raw_data['Size'].fillna(raw_data['Size'].median())

    return raw_data
    pass


clean_data = transform(merged_df)
#print(clean_data)

# the avg_weekly_sales_per_month function that takes in the cleaned data from the last step
def avg_weekly_sales_per_month(clean_data):

    # datetime format
    clean_data["Date"] = pd.to_datetime(clean_data["Date"])

    # Extracted Year & Month
    clean_data["Year-Month"] = clean_data["Date"].dt.to_period("M")  # 'YYYY-MM' format

    #  Group by and Calculated Average Weekly Sales
    monthly_avg_sales = clean_data.groupby("Year-Month")["Weekly_Sales"].mean().reset_index()

    monthly_avg_sales.rename(columns={"Weekly_Sales": "Avg_Weekly_Sales"}, inplace=True)

    return monthly_avg_sales
    
    pass


agg_data = avg_weekly_sales_per_month(clean_data)
print(agg_data)
pass

#  the load() function that takes in the cleaned DataFrame and the aggregated one with the paths where they are going to be stored
def load(full_data, full_data_file_path, agg_data, agg_data_file_path):

    """
    Saves the cleaned full dataset and the aggregated dataset to specified file paths.

    Parameters:
    - full_data: DataFrame, cleaned full dataset
    - full_data_file_path: str, path to save the full dataset
    - agg_data: DataFrame, aggregated dataset (e.g., monthly average sales)
    - agg_data_file_path: str, path to save the aggregated dataset
    """
    
    # Save the full cleaned dataset
    full_data.to_csv(full_data_file_path, index=False)  # Save as CSV without index
    full_data.to_parquet(full_data_file_path.replace(".csv", ".parquet"))  # Save as Parquet

    # Save the aggregated dataset
    agg_data.to_csv(agg_data_file_path, index=False)  
    agg_data.to_parquet(agg_data_file_path.replace(".csv", ".parquet"))  

    print(f"Data successfully saved to:\n- {full_data_file_path}\n- {agg_data_file_path}")

    pass

full_data_file_path = "clean_data.csv"
agg_data_file_path = "agg_data.csv"

load(clean_data, full_data_file_path, agg_data, agg_data_file_path)


# the validation() function with one parameter: file_path - to check whether the previous function was correctly executed

import os

def validation(file_path):
    """
    Checks if the file exists at the given path.

    Parameters:
    - file_path (str): Path to the file being validated.

    Returns:
    - str: Success message if file exists, error message if not.
    """
    
    if os.path.exists(file_path):
        print(f"✅ File successfully saved: {file_path}")
        return True
    else:
        print(f"❌ ERROR: File not found at {file_path}")
        return False
    pass


#full_data_file_path = "clean_data.csv"
#agg_data_file_path = "agg_data.csv"

# Validate both files
validation(full_data_file_path)  
validation(agg_data_file_path) 

