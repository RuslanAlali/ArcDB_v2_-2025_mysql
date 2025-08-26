import pandas as pd
from sqlalchemy import create_engine , text
import os

# Database connection string
con_string = 'mysql+pymysql://root:Asdqwe123@localhost/vcf_sql'
engine = create_engine(con_string)

# Define the output CSV file name
output_csv_file = 'merged_variant_data_hq.csv'

# Define the chunk size for reading data. Adjust this based on your available RAM.
# 100,000 rows is a good starting point for 86M rows, but you might go higher (e.g., 500,000)
# if you have plenty of RAM (e.g., 16GB or more).
CHUNK_SIZE = 100000

def export_merged_tables_to_csv(output_file: str, chunk_size: int):
    """
    Exports merged data from variant_counts and Variant tables to a CSV file in chunks.

    Args:
        output_file (str): The name of the output CSV file.
        chunk_size (int): The number of rows to fetch from the database at a time.
    """
    sample_number= (pd.read_sql_query("select count(*) from sample_counts", engine)).iloc[0,0]
    print(f"Number of samples = {sample_number}...")
    sample_number=sample_number//10
    print(f"Number of samples = {sample_number}...")

    print(f"Starting export to {output_file}...")


    # The SQL query to join the two tables and select the desired columns.
    # Ensure variantID is indexed in both tables for efficient joining.
    sql_query = text("""
    SELECT
        vc.variantID,
        vc.count,
        v.Chrom,
        v.Pos,
        v.Ref,
        v.Alt
    FROM
        variant_counts_hq AS vc
    INNER JOIN
        Variant AS v
    ON
        vc.variantID = v.variantID
    """)

    first_chunk = True
    total_rows_processed = 0

    try:
        # Use pandas read_sql_query with chunksize for memory efficiency
        for chunk_df in pd.read_sql_query(sql_query, engine, chunksize=chunk_size):
            if first_chunk:
                # Write header for the first chunk
                #output_file_clean=output_file.loc[output_file['count]> sample_number]
                chunk_df.to_csv(output_file, mode='w', index=False, header=True)
                first_chunk = False
            else:
                # Append subsequent chunks without header
                chunk_df.to_csv(output_file, mode='a', index=False, header=False)

            total_rows_processed += len(chunk_df)
            print(f"Processed {total_rows_processed} rows. Current chunk size: {len(chunk_df)}")

        print(f"Export completed successfully! Total rows exported: {total_rows_processed}")

    except Exception as e:
        print(f"An error occurred during export: {e}")
        # Clean up partially created file if an error occurs
        if os.path.exists(output_file):
            print(f"Removing partially created file: {output_file}")
            os.remove(output_file)
    finally:
        # Dispose of the engine to close all connections in the pool
        engine.dispose()
        print("Database connection closed.")

if __name__ == "__main__":
    export_merged_tables_to_csv(output_csv_file, CHUNK_SIZE)