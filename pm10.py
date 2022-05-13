import pandas as pd
import os





file_list = os.listdir("PM10")
for file_name in file_list:
    file_name = f"PM10/{file_name}"
    df = pd.read_excel(file_name, sheet_name='PM10')

    keep = []
    for dates in df['Date']:
        if pd.isnull(dates):
            break
        for station_code in df.columns[1:]:
            pm = df.loc[df['Date'] == dates, station_code].values[0]
            keep.append([dates, station_code, pm])

    df_result = pd.DataFrame(keep)
    df_result.columns = ['report_dt', 'station_code', 'pm10']

    for col in schema['list_of_col']:
        df_result[col['name']] = df_result[col['name']].astype(col['data_type'])
    df_result = df_result[[col['name'] for col in schema['list_of_col']]]
    df_result.to_parquet(f'{file_name.split(".")[0]}.parquet', index=False)
