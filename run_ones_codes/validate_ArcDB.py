import pandas as pd

#ArcDB1=pd.read_csv("raw/arcad_raw_noCutoff_250509.csv")
ArcDB1=pd.read_csv("raw/arcad_raw_noCutoff_250509.csv")
ArcDB2=pd.read_csv('merged_variant_data.csv')
ArcDB2_hq=pd.read_csv('merged_variant_data_hq.csv')
sample_DB2=3271

ArcDB1=ArcDB1.loc[ArcDB1['count']>(4861*5//100)]
ArcDB2=ArcDB2.loc[ArcDB2['count']>(sample_DB2*5//100)]
ArcDB2_hq=ArcDB2_hq.loc[ArcDB2_hq['count']>(sample_DB2*5//100)]

ArcDB1['variant']=ArcDB1['CHROM']+":"+ArcDB1['POS'].astype('string')+"_"+ArcDB1['REF']+"_"+ArcDB1['ALT']
ArcDB2['variant']=ArcDB2['Chrom']+":"+ArcDB2['Pos'].astype('string')+"_"+ArcDB2['Ref']+"_"+ArcDB2['Alt']
ArcDB2_hq['variant']=ArcDB2_hq['Chrom']+":"+ArcDB2_hq['Pos'].astype('string')+"_"+ArcDB2_hq['Ref']+"_"+ArcDB2_hq['Alt']

x=ArcDB1['variant']
set1 = set(x[x.str.len()<150])
x=ArcDB2['variant']
set2 = set(x[x.str.len()<150])
x=ArcDB2_hq['variant']
set2hq = set(x[x.str.len()<150])

# Find common and unique items
common_items = set1 & set2
unique_in_col1 = set1 - common_items
unique_in_col2 = set2 - common_items

print(f"Common items: {len(common_items)} ")
print(f"Unique in ArcDB1: {len(unique_in_col1)} ")
print(f"Unique in ArcDB2: {len(unique_in_col2)} ")

ArcDB1x = ArcDB1[ArcDB1['variant'].isin(unique_in_col1)]
ArcDB1x.to_excel('unique_ArcDB1.xlsx',index=False)

common_items = set1 & set2hq
unique_in_col1 = set1 - common_items
unique_in_col2 = set2hq - common_items

print(f"Common items: {len(common_items)} ")
print(f"Unique in ArcDB1: {len(unique_in_col1)}")
print(f"Unique in ArcDB2HQ: {len(unique_in_col2)}")
