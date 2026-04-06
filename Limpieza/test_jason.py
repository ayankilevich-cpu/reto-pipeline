import pandas as pd
import json

df = pd.read_csv("/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Limpieza/v_message_anonymized.csv")

def extract_field(row, path):
    try:
        data = json.loads(row)
        for p in path.split("."):
            data = data[p]
        return data
    except Exception as e:
        print("ERROR:", e)
        print("ROW:", row[:200])
        return None

print("video_id:", extract_field(df['extra'][0], "raw.snippet.videoId"))
print("text_original:", extract_field(df['extra'][0], "raw.snippet.topLevelComment.snippet.textOriginal"))
