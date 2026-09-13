import requests

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
year = 2026
for month in range(1,4):
    month_str = f"{month:02d}"
    file_name = f"yellow_tripdata_{year}-{month_str}.parquet"
    url = base_url + file_name 
    output_path = f"data/raw/{file_name}"
    print(f"downloding{file_name}...")
    response = requests.get(url)
    response.raise_for_status()
    with open(output_path , "wb") as file :
        file.write(response.content)

    print (f"{file_name}downlode complited")

