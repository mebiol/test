import pandas as pd
import os


table_name = "PM10"
schema = {
    "table_name": table_name,
    "bucket": "location-intelligence-feature-store",
    "data_path": f"curated/{table_name}",
    "desc_path": f"description/{table_name}/{table_name}.json",
    "list_of_col": [
        {
            "name": "report_dt",
            "data_type": "datetime64",
            "desc_th": "วันที่ได้รับรายงานข้อมูล",
            "desc_en": "reported date"
        },
        {
            "name": "station_code",
            "data_type": "str",
            "desc_th": "รหัสสถานี",
            "desc_en": "station code"
        },
        {
            "name": "pm10",
            "data_type": "float",
            "desc_th": "ฝุ่นละอองที่มีขนาดเล็กกว่า 10",
            "desc_en": "Particulate Matters 10"
        }
    ]
}


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
