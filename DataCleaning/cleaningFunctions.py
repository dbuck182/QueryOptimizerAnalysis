import pandas as pd

#df = pd.read_csv("/Users/drewbuck/Desktop/imdbReal/imdb/aka_name.csv")

input_path = "/Users/drewbuck/Desktop/imdbReal/imdb/title.csv"
output_path = "/Users/drewbuck/Desktop/imdbReal/imdb/title_clean.csv"

with open(input_path, "r", encoding="utf-8") as infile, open(output_path, "w", encoding="utf-8") as outfile:
    for line in infile:
        cleaned = line.replace('\\"', '""')
        outfile.write(cleaned)

print("Cleaned file saved to:", output_path)



#print(df.nrows)