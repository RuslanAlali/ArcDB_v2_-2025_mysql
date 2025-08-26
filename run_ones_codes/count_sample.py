# Import the necessary libraries
import pandas as pd
from sqlalchemy import create_engine
import logging

# Configure logging for better error handling and progress tracking
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def count_sample_frequencies():
    """
    Connects to a MySQL database, counts the frequency of the 'sample' column
    in a large partitioned table, and saves the results to a new table.
    """
    # Define the connection string.
    # The 'con_string' must be a valid SQLAlchemy connection string.
    con_string = 'mysql+pymysql://root:Asdqwe123@localhost/vcf_sql'
    
    # Table names
    input_table = 'VCF_detection'
    output_table = 'sample_counts'
    
    try:
        # Create a database engine
        engine = create_engine(con_string)
        
        logging.info(f"Connected to the database.")

        # Step 1: Get all unique partition IDs from the table.
        logging.info("Fetching unique partition IDs...")
        partition_query = f"SELECT DISTINCT partitionID FROM {input_table}"
        
        # Use a chunksize to avoid loading all IDs into memory at once if there are many partitions
        partition_ids_df = pd.read_sql_query(partition_query, engine, chunksize=50000)
        
        # Step 2: Initialize a pandas Series to accumulate the total counts.
        # This is more memory-efficient than storing a list of DataFrames.
        final_counts_series = pd.Series(dtype='int64')

        # Iterate through the chunks of partition IDs
        for chunk in partition_ids_df:
            partition_ids = chunk['partitionID'].tolist()
            logging.info(f"Processing a chunk of {len(partition_ids)} partitions.")
            
            # Loop through each partition and get sample counts.
            for partition_id in partition_ids:
                logging.info(f"Counting samples for partition: {partition_id}")
                
                # Construct the query to count sample frequencies for the current partition.
                counts_query = (
                    f"SELECT sample, COUNT(*) AS count "
                    f"FROM {input_table} "
                    f"WHERE partitionID = '{partition_id}' "
                    f"GROUP BY sample"
                )
                
                # Read the results directly into a pandas DataFrame.
                df_counts = pd.read_sql_query(counts_query, engine)
                
                # Step 3: Accumulate the counts for this partition into the final Series.
                # The .add() method with fill_value=0 handles samples that may not exist in every partition
                # by treating their count as 0 if not present.
                if not df_counts.empty:
                    df_counts_series = df_counts.set_index('sample')['count']
                    final_counts_series = final_counts_series.add(df_counts_series, fill_value=0)
        
        # Step 4: Convert the final Series to a DataFrame for saving.
        if not final_counts_series.empty:
            logging.info("Converting final Series to DataFrame...")
            final_counts_df = final_counts_series.reset_index()
            final_counts_df.columns = ['sample', 'count']
            
            # Step 5: Write the final results to the new table.
            logging.info(f"Writing final counts to '{output_table}' table...")
            final_counts_df.to_sql(
                output_table,
                engine,
                if_exists='replace', # This will drop the table and re-create it.
                index=False
            )
            
            logging.info("Successfully completed the task.")
            print(f"Final counts have been saved to the '{output_table}' table.")
            print("\nPreview of the final counts:")
            print(final_counts_df.head()) # Print the first few rows for confirmation
        else:
            logging.warning("No data found to process. The input table might be empty.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

# Run the function
if __name__ == "__main__":
    count_sample_frequencies()
