import os
import pandas as pd



def removing_rows(root_dir):
    for file in os.listdir(root_dir):
        if file.endswith('.csv'):
            file_path = os.path.join(root_dir, file) #creates path

            df = pd.read_csv(file_path)
            df = df.iloc[:-1]  # drop last row

            df.to_csv(file_path, index=False)  #back to csv
            print(f'{file} saved')