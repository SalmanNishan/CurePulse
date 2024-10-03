import pandas as pd

# Load the Excel file
file_path = 'Updated_practice_name_and_number.xlsx'  # Update with your file path
df = pd.read_excel(file_path)

# Create a new DataFrame for the transformed data
new_data = []

# Get unique practice names
unique_practices = df['Client Names'].unique()

for practice in unique_practices:
    practice_rows = df[df['Client Names'] == practice]
    
    # Get the practice phone number (assuming it's the same for all rows with the same practice name)
    practice_phone_number = practice_rows['Client Numbers'].iloc[0]
    
    # Add the main practice entry
    new_data.append({
        'Client Names': practice,
        'Client Numbers': practice_phone_number
    })
    
    # Add the combined entries for location names
    for index, row in practice_rows.iterrows():
        if row['Location Name'] != practice:
            combined_name = f"{practice}, {row['Location Name']}"
            location_phone_number = row['Location Phone Number']
            new_data.append({
                'Client Names': combined_name,
                'Client Numbers': location_phone_number
            })

# Create a new DataFrame from the transformed data
new_df = pd.DataFrame(new_data)

# Save the new DataFrame to a new Excel file
new_file_path = 'updated_client_numbers.csv'
new_df.to_csv(new_file_path, index=False)

print(f"Transformed data has been saved to {new_file_path}")