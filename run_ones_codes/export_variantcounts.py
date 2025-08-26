import pandas as pd
from sqlalchemy import create_engine

# One-liner to export entire table
pd.read_sql_table('variant_counts_hq', create_engine('mysql+pymysql://root:Asdqwe123@localhost/vcf_sql')).to_csv('variant_counts_hq100.csv', index=False)
pd.read_sql_table('variant_counts', create_engine('mysql+pymysql://root:Asdqwe123@localhost/vcf_sql')).to_csv('variant_counts.csv', index=False)