import csv
import json
import ast

index_name = "animes"
input_csv = "dataset/anime_cleaned.csv"
output_ndjson = "animes_bulk.ndjson"

def to_int(value):
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None

def to_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def to_bool(value):
    if value is None:
        return None
    s = str(value).strip().lower()
    if s in ("true", "t", "1", "yes"):
        return True
    if s in ("false", "f", "0", "no"):
        return False
    return None

def to_list(string: str, sep: str=",") -> list:
    try:
        return_list = string.split(sep)
        return_list = [string.strip() for string in return_list]

        return return_list
    except (ValueError, TypeError):
        return None

def to_object(value):
    if not value:
        return None
    try:
        # transforma a string "{'Adaptation': [...], 'Sequel': [...]}"
        # em dict Python com listas dentro
        obj = ast.literal_eval(value)

        if type(obj) == dict:
            keys_related_mangas = []
            for (key, val) in obj.items():
                if val[0]['type'] == 'manga':
                    keys_related_mangas.append(key)
            
            for key in keys_related_mangas:
                del obj[key]

            return obj
        
        return None
    except (ValueError, SyntaxError):
        return None

if __name__ == "__main__":
    with open(input_csv, newline="", encoding="utf-8") as csvfile, \
        open(output_ndjson, "w", encoding="utf-8") as outfile:

        reader = csv.DictReader(csvfile)

        for row in reader:
            # Conversões de tipo
            row["anime_id"]        = to_int(row.get("anime_id"))
            row["episodes"]        = to_int(row.get("episodes"))
            row["airing"]          = to_bool(row.get("airing"))
            row["score"]           = to_float(row.get("score"))
            row["scored_by"]       = to_int(row.get("scored_by"))
            row["rank"]            = to_float(row.get("rank"))
            row["popularity"]      = to_int(row.get("popularity"))
            row["members"]         = to_int(row.get("members"))
            row["favorites"]       = to_int(row.get("favorites"))
            row["duration_min"]    = to_float(row.get("duration_min"))
            row["aired_from_year"] = to_int(row.get("aired_from_year"))

            row['genre'] = to_list(row.get("genre"))

            if 'Hentai' in row['genre'] or 'Yaoi' in row['genre'] or 'nudity' in row['rating'].lower():
                continue

            row['studio'] = to_list(row.get("studio"))
            row['producer'] = to_list(row.get("producer"))

            row['related'] = to_object(row.get("related"))

            deleting_cols = ['opening_theme', 'ending_theme', 'title_japanese', 'source', 'status', 'aired_string', 'aired', 'duration']
            for col in deleting_cols:
                del row[col]

            anime_id = row["anime_id"]

            # Linha de ação do bulk com _id = anime_id
            action = { "index": { "_index": index_name, "_id": anime_id } }
            outfile.write(json.dumps(action, ensure_ascii=False) + "\n")
            outfile.write(json.dumps(row, ensure_ascii=False) + "\n")
