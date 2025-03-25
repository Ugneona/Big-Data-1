def calculate_distance_matrix(latitudes, longitudes):
    """
    Apskaičiuoja atstumų matricą tarp visų laivų.
    """
    R = 6371  # Žemės spindulys kilometrais
    lat_rad = np.radians(latitudes)
    lon_rad = np.radians(longitudes)

    lat_diff = lat_rad[:, None] - lat_rad[None, :]
    lon_diff = lon_rad[:, None] - lon_rad[None, :]
    
    a = np.sin(lat_diff / 2) ** 2 + np.cos(lat_rad[:, None]) * np.cos(lat_rad[None, :]) * np.sin(lon_diff / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    
    return (R * c) / 1.852  # Konvertuojame į jūrmyles

def assign_location_groups(df, distance_threshold_nm=10):
    """
    Priskiria laivams grupės numerį pagal jų artumą vienas kitam,
    nenaudojant `combinations`.
    """
    df = df.copy()
    df["location_group"] = -1  # Pradžioje visi laivai neturi grupės
    group_id = 0

    latitudes = df["Latitude"].to_numpy()
    longitudes = df["Longitude"].to_numpy()

    distance_matrix = calculate_distance_matrix(latitudes, longitudes)  # Atstumų matrica

    assigned = np.full(len(df), False)  # Sekame, kurie laivai jau priskirti grupei

    for i in range(len(df)):
        if not assigned[i]:  # Jei laivas dar neturi grupės
            mask = distance_matrix[i] < distance_threshold_nm  # Randame artimus laivus
            df.loc[mask, "location_group"] = group_id  # Priskiriame tą pačią grupę
            assigned[mask] = True  # Pažymime, kad laivai jau priskirti
            group_id += 1

    return df

@tw.timeit
def task_C(sub_df):
    sub_df = sub_df.copy()
    grouped_data = [group for _, group in sub_df.groupby('# Timestamp')] 
    group1=grouped_data[0]
    group2=grouped_data[1]

    if len(group1) > 2:  
        sub_df["spoofing_c"] = False
        
        group1 = assign_location_groups(group1)

        if group1["location_group"].any() != -1:
            for group, _ in range(len(group1["location_group"].unique()):
                for mmsi in len(group1[group1['location_group'] == group]['MMSI'].unique()):
                    lat0, lon0 = group1.iloc[group1['location_group' == group]]['Latitude'], group1.iloc[group1['location_group' == group]]['Longitude']
                    lat1, lon1 = group2.iloc[group1['location_group' == group]]['Latitude'], group2.iloc[i]['Longitude']

