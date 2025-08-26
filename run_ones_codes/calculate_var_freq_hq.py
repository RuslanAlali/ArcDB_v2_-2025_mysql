import math
from sqlalchemy import create_engine, text, Column, Integer, String, BigInteger
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base

# Database connection string
con_string = 'mysql+pymysql://root:Asdqwe123@localhost/vcf_sql'
engine = create_engine(con_string)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Define the tables
class Variant(Base):
    __tablename__ = 'Variant'
    variantID = Column(Integer, primary_key=True)
    Chrom = Column(String(50))
    Pos = Column(BigInteger)

class VcfDetection(Base):
    __tablename__ = 'VCF_detection'
    # Assuming a composite primary key or an auto-incrementing ID
    id = Column(Integer, primary_key=True) # Assuming an ID column for primary key
    variantID = Column(Integer)
    partitionID = Column(String(255)) # Store the partition name, e.g., 'chr1_1'

class VariantCounts(Base):
    __tablename__ = 'variant_counts_hq'
    variantID = Column(BigInteger, primary_key=True)
    partitionID = Column(String(255), nullable=False) # partitionID is essential and should not be null
    count = Column(BigInteger, nullable=False, default=0)

# Create tables if they don't exist (for testing purposes)
# In a production environment, you might not want to create/drop tables programmatically
# Base.metadata.create_all(engine)

def get_partition_name(chrom, pos, interval=50000000):
    """Calculates the partition name based on Chrom and Pos."""
    return f"{chrom}_{math.floor(pos / interval)}"

def process_variants_and_counts():
    session = Session()
    try:
        # 1. Pre-calculate partition_name for all variants in the Variant table
        # We'll store this in a dictionary for quick lookup
        print("Fetching variant data and pre-calculating partition names...")
        variant_to_partition = {}
        variants = session.query(Variant.variantID, Variant.Chrom, Variant.Pos).all()
        for var_id, chrom, pos in variants:
            variant_to_partition[var_id] = get_partition_name(chrom, pos)
        print(f"Pre-calculated partition names for {len(variant_to_partition)} variants.")

        # 2. Iterate through partitions of vcf_detection
        # Since we have 'partitionID' in vcf_detection, we can group by it.
        # However, to explicitly use the partition-aware queries,
        # we would ideally have a way to list the actual partition names
        # from the database's internal partitioning scheme.
        # Given the problem description, 'partitionID' in vcf_detection table
        # already represents this.

        # Get a list of all unique partitionIDs from vcf_detection
        print("Fetching unique partition IDs from vcf_detection...")
        unique_partition_ids_query = text("SELECT DISTINCT partitionID FROM VCF_detection")
        unique_partition_ids = [row[0] for row in session.execute(unique_partition_ids_query)]
        print(f"Found {len(unique_partition_ids)} unique partitions.")

        total_variants_processed = 0
        batch_size = 50000 # Adjust batch size based on memory and database performance
        variant_counts_to_insert = []

        for partition_id in unique_partition_ids:
            print(f"Processing partition: {partition_id}")
            # Count variants within each partition
            # We assume 'partitionID' column in vcf_detection directly maps to the physical partition.
            # For a real partitioned table, a WHERE clause on the partitioning column is usually enough
            # for the database to prune partitions.
            #QUAL>100
            count_query = text(
                f"SELECT variantID, COUNT(*) AS count FROM VCF_detection WHERE partitionID = :pid AND QUAL>100 GROUP BY variantID"
            )
            results = session.execute(count_query, {"pid": partition_id}).fetchall()

            for variant_id, count in results:
                # Add to batch for bulk insert
                variant_counts_to_insert.append({"variantID": variant_id, "partitionID": partition_id, "count": count})
                total_variants_processed += 1

                if len(variant_counts_to_insert) >= batch_size:
                    print(f"Inserting batch of {len(variant_counts_to_insert)} variant counts...")
                    session.bulk_insert_mappings(VariantCounts, variant_counts_to_insert)
                    session.commit()
                    variant_counts_to_insert = [] # Clear the batch
                    print(f"Total variants processed so far: {total_variants_processed}")

        # Insert any remaining counts
        if variant_counts_to_insert:
            print(f"Inserting final batch of {len(variant_counts_to_insert)} variant counts...")
            session.bulk_insert_mappings(VariantCounts, variant_counts_to_insert)
            session.commit()

        print(f"Completed processing. Total unique variants counted: {total_variants_processed}")

    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    # Ensure the variant_counts table exists and is empty before starting
    # You might want to run this once manually or in a migration script
    # with Session() as s:
    #     s.execute(text("DROP TABLE IF EXISTS variant_counts;"))
    #     Base.metadata.create_all(engine) # Recreate if dropped

    process_variants_and_counts()