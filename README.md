def task_B(sub_df):
    sub_df = sub_df.sort_values(by='# Timestamp').copy()

    if len(sub_df) > 2:  
        sub_df["spoofing_b"] = False

        for i in range(1, len(sub_df) - 2):  
            sog0, sog1, sog2 = sub_df.iloc[i - 1]['SOG'], sub_df.iloc[i]['SOG'], sub_df.iloc[i + 1]['SOG']
            cog0, cog1, cog2 = sub_df.iloc[i - 1]['COG'], sub_df.iloc[i]['COG'], sub_df.iloc[i + 1]['COG']

            lat0, lon0 = sub_df.iloc[i - 1]['Latitude'], sub_df.iloc[i - 1]['Longitude']
            lat1, lon1 = sub_df.iloc[i]['Latitude'], sub_df.iloc[i]['Longitude']
            lat2, lon2 = sub_df.iloc[i + 1]['Latitude'], sub_df.iloc[i + 1]['Longitude']

            if pd.notna([sog0, sog1, sog2, lat0, lon0, lat1, lon1, lat2, lon2]).all():  
                distance_nm0 = calculate_distance(lat0, lon0, lat1, lon1) / 1.852
                distance_nm1 = calculate_distance(lat1, lon1, lat2, lon2) / 1.852
                time_diff0 = (sub_df.iloc[i]['# Timestamp'] - sub_df.iloc[i - 1]['# Timestamp']).total_seconds() / 60

                if time_diff0 > 0:
                    if ((abs(sog0 - sog1) > 10 and abs(sog1 - sog2) > 10) or
                        (abs(sog0 - sog1) > 90 and abs(sog2 - sog1) > 90) or
                        (distance_nm0 > 10 and distance_nm1 > 10)):  
                        sub_df.at[sub_df.index[i], 'spoofing_b'] = True
    else:
        sub_df["spoofing_b"] = None

    return sub_df
