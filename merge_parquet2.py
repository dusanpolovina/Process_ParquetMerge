import os
import pyarrow.parquet as pq

#
# Warning!!!
# Suffers from the same problem as the parquet-tools merge function
#
#parquet-tools merge:
#Merges multiple Parquet files into one. The command doesn't merge row groups,
#just places one after the other. When used to merge many small files, the
#resulting file will still contain small row groups, which usually leads to bad
#query performance.

def combine_parquet_files(input_folder, target_path):
    try:
        files = []
        for file_name in os.listdir(input_folder):
            files.append(pq.read_table(os.path.join(input_folder, file_name)))
        with pq.ParquetWriter(target_path,
                files[0].schema,
                version='2.0',
                compression='gzip',
                use_dictionary=True,
                data_page_size=2097152, #2MB
                write_statistics=True) as writer:
            for f in files:
                writer.write_table(f)
    except Exception as e:
        print(e)

input_folder='C:\\Users\\dpolovi2\\Documents\\MATLAB\\ME F150 (061w449)\\MABx Data Logger Files (MF4)\\Test\\Export\\parquet_merge'
target_path='C:\\Users\\dpolovi2\\Documents\\MATLAB\\ME F150 (061w449)\\MABx Data Logger Files (MF4)\\Test\\Export\\combined.parquet'
combine_parquet_files(input_folder, target_path)