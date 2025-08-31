import pandas as pd

try:
    flood_inventory_df = pd.read_csv(r'C:\Users\rahul\OneDrive\Desktop\AICTE\rain\data\India_Flood_Inventory_v3.csv')
    rainfall_ll_df = pd.read_csv(r'C:\Users\rahul\OneDrive\Desktop\AICTE\rain\data\Rainfall_Data_LL.csv')
    flood_inventory_df.columns = flood_inventory_df.columns.str.strip()
    columns_list = list(flood_inventory_df.columns)
    column_to_drop = [col for col in columns_list if 'Extent of damage' in col]
    columns_to_drop = [
        'Unnamed: 0', 'Location', 'Latitude', 'Longitude', 'Severity',
        'Area Affected', 'Event Souce ID', 'Description of Casualties/injured',
        'Districts'
    ]
    columns_to_drop.extend(column_to_drop)

    flood_inventory_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    flood_inventory_df['Start Date'] = pd.to_datetime(flood_inventory_df['Start Date'], dayfirst=True, errors='coerce')
    flood_inventory_df['End Date'] = pd.to_datetime(flood_inventory_df['End Date'], dayfirst=True, errors='coerce')
    flood_inventory_df['YEAR'] = flood_inventory_df['Start Date'].dt.year

    numeric_cols = ['Human Displaced', 'Animal Fatality', 'Human injured', 'Human fatality']
    for col in numeric_cols:
        if col in flood_inventory_df.columns:
            flood_inventory_df[col] = pd.to_numeric(flood_inventory_df[col], errors='coerce').fillna(0)

    flood_inventory_df['Duration(Days)'] = (flood_inventory_df['End Date'] - flood_inventory_df['Start Date']).dt.days.fillna(0)

    if 'State' in flood_inventory_df.columns:
        flood_inventory_df['State'] = flood_inventory_df['State'].str.strip().str.upper()
    else:
        raise KeyError("Column 'State' not found in flood inventory data.")

    rainfall_ll_df.rename(columns={'SUBDIVISION': 'State'}, inplace=True)
    if 'Name' in rainfall_ll_df.columns:
        rainfall_ll_df.drop(columns=['Name'], inplace=True)
    rainfall_ll_df['State'] = rainfall_ll_df['State'].str.strip().str.upper()

    
    final_merged_df = pd.merge(flood_inventory_df, rainfall_ll_df, on=['State', 'YEAR'], how='left')

    # Check for missing rainfall data
    missing_rainfall = final_merged_df.isna().sum().get('Rainfall', 0)
    print(f"Number of flood records with missing rainfall data: {missing_rainfall}")

    # Drop duplicates
    final_merged_df.drop_duplicates(inplace=True)

    # --- Save Final Merged Dataset ---
    final_merged_df.to_csv(r'C:\Users\rahul\OneDrive\Desktop\AICTE\rain\data\merged_flood_detection_dataset_fixed.csv', index=False)

    # --- Output Info ---
    print("Final Merged Dataset Info:")
    print(final_merged_df.info())
    print("\nFirst 5 rows of the Final Merged Dataset:")
    print(final_merged_df.head())

except FileNotFoundError as e:
    print(f"Error: {e}. Please ensure the directory and file names are correct.")
except KeyError as e:
    print(f"Column error: {e}. Please check the dataset columns.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
