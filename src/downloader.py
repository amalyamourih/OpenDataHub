import requests
import os


def download_file(list_resource, dest_path_data_temp=f"data_temp/"):
    for resource in list_resource:
        url = resource[1]
        dest_path = resource[2]
        file_name = os.path.basename(url)
        os.makedirs(dest_path_data_temp + dest_path, exist_ok=True)
        
        os.makedirs(os.path.dirname(dest_path_data_temp + dest_path), exist_ok=True)
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(dest_path_data_temp + dest_path + file_name, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
    return dest_path_data_temp


        
        
