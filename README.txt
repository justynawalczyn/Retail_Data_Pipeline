# Retail_Data_Pipeline
Repository contains the project "Building new retail data pipeline" based on datacamp.com project task.


Builded a data pipeline using custom functions to extract, transform, aggregate, and load e-commerce data.

DATA:

grocery_sales
"index" - unique ID of the row
"Store_ID" - the store number
"Date" - the week of sales
"Weekly_Sales" - sales for the given store
Also, you have the extra_data.parquet file that contains complementary data:

extra_data.parquet
"IsHoliday" - Whether the week contains a public holiday - 1 if yes, 0 if no.
"Temperature" - Temperature on the day of sale
"Fuel_Price" - Cost of fuel in the region
"CPI" – Prevailing consumer price index
"Unemployment" - The prevailing unemployment rate
"MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4" - number of promotional markdowns
"Dept" - Department Number in each store
"Size" - size of the store
"Type" - type of the store (depends on Size column)

merged those files and performed some data manipulations. The transformed DataFrame can then be stored as the clean_data variable containing the following columns:


"Store_ID"
"Month"
"Dept"
"IsHoliday"
"Weekly_Sales"
"CPI"
""Unemployment""
After merging and cleaning the data,  analyzed monthly sales of Walmart and stored the results of  analysis as the agg_data variable that  look like:

Month	Weekly_Sales
1.0	33174.178494
2.0	34333.326579
...	...

Builded a data pipeline using custom functions to extract, transform, aggregate, and load e-commerce data.

