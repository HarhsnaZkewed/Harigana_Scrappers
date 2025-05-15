import pymongo
import pandas as pd
import re
from datetime import datetime, timedelta

# Connection to MongoDB Atlas
client = pymongo.MongoClient("mongodb+srv://zkewed:zkewed123A@vehicalevaluation.d9ufa.mongodb.net/?retryWrites=true&w=majority", 27017)
#client = pymongo.MongoClient('mongodb+srv://harshanabuddhika9:uh4Av1QRBqmhXjwL@cluster0.bgvrx7w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')


#Connection to the un-processed DB
db = client['data_store_dev']

# Collections
ikman_db = db['ikman_land_tb']
patpat_db = db['patpat_tb']
lanakaproperty_db = db['lanakaproperty_tb']
combined_db = db['combined_tb']

# Define the threshold time
threshold_time1 = datetime.now() - timedelta(minutes=60)
threshold_time2 = datetime.now() - timedelta(days=90)

# Function to check count before and after deletion
def delete_old_records(collection, threshold):
    count_before = collection.count_documents({"inserted_datetime": {"$lt": threshold}})
    print(f"Records before deletion in {collection.name}: {count_before}")
    
    result = collection.delete_many({"inserted_datetime": {"$lt": threshold}})
    
    count_after = collection.count_documents({"inserted_datetime": {"$lt": threshold}})
    print(f"Records deleted: {result.deleted_count}")
    print(f"Records after deletion in {collection.name}: {count_after}")

# Delete old records
delete_old_records(ikman_db, threshold_time1)
delete_old_records(patpat_db, threshold_time1)
delete_old_records(lanakaproperty_db, threshold_time1)
delete_old_records(combined_db, threshold_time2)

print("Old records deleted successfully.")

# Convert MongoDB data to DataFrameres
ikman_pd = pd.DataFrame(list(ikman_db.find()))
patpat_pd = pd.DataFrame(list(patpat_db.find()))
lanakaproperty_pd = pd.DataFrame(list(lanakaproperty_db.find()))


def extract_property_types(type):
    if type:
        house_match = re.search(r'(Houses|House)', type, re.IGNORECASE)
        land_match = re.search(r'(Lands|Land)', type, re.IGNORECASE)
        apartment_match = re.search(r'(Apartments|Apartment)', type, re.IGNORECASE)
        rental_match = re.search(r'(Rental|Rentals)', type, re.IGNORECASE)

        if house_match:
            return 'House'
        elif land_match:
            return "Land"
        elif apartment_match:
            return "Apartment"
        elif rental_match:
            return "Rental"
        else:
            return None
    else:
        return None


def extract_perches(land_area):
    if land_area:

        # Regular expression to find numbers followed by units (perch, perches, acre, acres)
        match = re.search(r'(\d+(\.\d+)?)\s*(perch|perches|acre|acres)', land_area, re.IGNORECASE)
        if match:
            value = float(match.group(1))  # Extract the numeric value
            unit = match.group(3).lower()  # Extract the unit
            if unit in ['acre', 'acres']:
                return value * 160  # Convert acres to perches
            elif unit in ['perch', 'perches']:
                return value  # No conversion needed for perches
        return None  # Return None if no match is found
    else:
        return land_area


def extract_unit_prices(price, perch):
    price = str(price) if price else ""

    # Check if the price already includes "per perch" and return as is
    match = re.search(r'(\d+(\.\d+)?)\s*(per perch)', price, re.IGNORECASE)
    if match:
        return price

    # If perch is provided, calculate the unit price per perch
    if perch:
        try:
            # Attempt to find a numeric price
            match = re.search(r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', price)
            if match:
                tot_price = float(match.group(1).replace(',', ''))

                # Ensure perch is numeric and non-zero before division
                perch_value = float(perch)
                if perch_value > 0:
                    unit_price = "{:,.2f}".format(tot_price / perch_value)
                    return f"Rs {unit_price} per perch"
                else:
                    return price  # Handle invalid perch value
            else:
                return None  # Return None if no price is found
        except ValueError:
            return price  # Handle conversion errors
    else:
        return price  # Return original price if no perch is given


#---------------------------------------------------------------------------------------------------------------------------------------------------------------

# Data Extraction Functions
def ikman_data_extraction():
    url = ikman_pd['url']
    title = ikman_pd['title']
    location = ikman_pd['location']
    bedrooms = ikman_pd['bedrooms']
    bathrooms = ikman_pd['bathrooms']
    floor_area = ikman_pd['floor_area']
    land_area = ikman_pd['land_area']
    price = ikman_pd['price']
    property_details = ikman_pd['property_details']
    property_features = ikman_pd['features']
    property_type = ikman_pd['property_type']
    inserted_datetime = ikman_pd['inserted_datetime']
    website = 'Ikaman'

    location = location + ',' + property_type

    # Clean property_features for each row
    property_features = property_features.apply(lambda features:
                                                [re.sub(r'[^\w\s.:/]', '', feature).strip() for feature in features if
                                                 isinstance(feature, str) and feature.strip()]
                                                if isinstance(features, list) else []
                                                )



    extracted_df = pd.DataFrame({
        'url': url,
        'title': title,
        'location': location,
        'bedrooms': bedrooms,
        'bathrooms': bathrooms,
        'floor_area': floor_area,
        'land_area': land_area,

        'price': price,

        'property_details': property_details,
        'property_features': property_features,
        'property_type': property_type,
        'inserted_datetime':inserted_datetime,
        'website': website
    })

    return extracted_df

def patpat_data_extraction():
    url = patpat_pd['url']
    title = patpat_pd['title']
    location = patpat_pd['location']
    bedrooms = patpat_pd['bedrooms']
    bathrooms = patpat_pd['bathrooms']
    floor_area = patpat_pd['floor_area']
    land_area = patpat_pd['land_area']
    price = patpat_pd['price']
    property_details = patpat_pd['property_details']
    property_features = patpat_pd['features']
    property_type = patpat_pd['property_type']
    inserted_datetime = patpat_pd['inserted_datetime']
    website = 'Pat Pat'

    property_features = property_features.apply(lambda features:
                                                [re.sub(r'[^\w\s.:/]', '', feature).strip() for feature in features if
                                                 isinstance(feature, str) and feature.strip()]
                                                if isinstance(features, list) else []
                                                )

    extracted_df = pd.DataFrame({
        'url': url,
        'title': title,
        'location': location,
        'bedrooms': bedrooms,
        'bathrooms': bathrooms,
        'floor_area': floor_area,
        'land_area': land_area,

        'price': price,

        'property_details': property_details,
        'property_features': property_features,
        'property_type': property_type,
        'inserted_datetime':inserted_datetime,
        'website': website
    })

    return extracted_df

def lanakaproperty_data_extraction():
    url = lanakaproperty_pd['url']
    title = lanakaproperty_pd['title']
    location = lanakaproperty_pd['location']
    bedrooms = lanakaproperty_pd['bedrooms']
    bathrooms = lanakaproperty_pd['bathrooms']
    floor_area = lanakaproperty_pd['floor_area']
    land_area = lanakaproperty_pd['land_area']
    price = lanakaproperty_pd['price']
    property_details = lanakaproperty_pd['property_details']
    property_features = lanakaproperty_pd['features']
    property_type = lanakaproperty_pd['property_type']
    inserted_datetime = lanakaproperty_pd['inserted_datetime']
    website = 'Lanka Property'

    property_features = property_features.apply(lambda features:
                                                [re.sub(r'[^\w\s.:/]', '', feature).strip() for feature in features if
                                                 isinstance(feature, str) and feature.strip()]
                                                if isinstance(features, list) else []
                                                )


    extracted_df = pd.DataFrame({
        'url': url,
        'title': title,
        'location': location,
        'bedrooms': bedrooms,
        'bathrooms': bathrooms,
        'floor_area': floor_area,
        'land_area': land_area,

        'price': price,

        'property_details': property_details,
        'property_features': property_features,
        'property_type': property_type,
        'inserted_datetime':inserted_datetime,
        'website': website
    })

    return extracted_df

# Extract DataFrames
ikman_df = ikman_data_extraction()
patpat_df = patpat_data_extraction()
lankaproperty_df = lanakaproperty_data_extraction()


# Combine DataFrames
combined_df = pd.concat([ikman_df, patpat_df, lankaproperty_df], ignore_index=True)

# Apply the updated extract_perches function to 'land_area'
combined_df['property_type'] = combined_df['property_type'].apply(lambda x: extract_property_types(x) if isinstance(x, str) else None)
combined_df['perches'] = combined_df['land_area'].apply(lambda x: extract_perches(x) if isinstance(x, str) else None)
combined_df['unit_price'] = combined_df.apply(
    lambda row: extract_unit_prices(row['price'], row['perches']) if row['property_type'] == 'Land' else row['price'],
    axis=1
)
combined_df["inserted_datetime"] = pd.to_datetime(combined_df["inserted_datetime"], errors='coerce')  # Coerce invalid values

# Convert DataFrame to list of dictionaries
combined_records = combined_df.to_dict(orient='records')

# Insert data into the 'combined_tb' collection
combined_db.insert_many(combined_records)

print("Data has been inserted into the 'processed_tb' collection on MongoDB Atlas.")
