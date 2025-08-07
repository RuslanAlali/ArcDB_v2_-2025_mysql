#!/usr/bin/env python
'''
__author__ = "Ruslan Alali"
__copyright__ = "Copyright 2024-2025, Arcensus GMBH"
use: conda activate python3
'''

import pandas as pd
import gzip
import time
import mysql.connector
import os
import sqlalchemy
from sqlalchemy import create_engine
import sys

pd.options.mode.chained_assignment = None #turn off warnings

con_string='mysql+pymysql://root:Asdqwe123@localhost/vcf_sql'
engine=create_engine(con_string)

def main():
    # Check if argument was provided
    if len(sys.argv) < 2:
        print("Please provide vcf file name")
        sys.exit(1)
    # Get the input argument (the first argument after script name)
    vcf_file = sys.argv[1]
    return vcf_file


def vcf_to_dataframe(vcf_file):
    timex=time.time()
    # Read the VCF file line by line, skipping header lines
    with gzip.open(vcf_file, 'rt') as f:
        lines = []
        for line in f:
            if not line.startswith('#'):
                lines.append(line.strip().split('\t'))
    # Create a DataFrame from the lines
    df = pd.DataFrame(lines)
    #print("read:" + str(int(time.time()-timex)))
    # Assign column names based on the VCF header (assuming standard VCF format)
    column_names = ['CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO', 'FORMAT', os.path.basename(vcf_file).split(".")[0]]
    df.columns = column_names
    # Remove the first row (header row)
    #df = df.iloc[1:]
    df=df[df['FILTER']=="PASS"]
    df=df.drop(['FILTER', 'ID'], axis=1)
    sample_name=os.path.basename(vcf_file).split("_")[0]
    print('sample_name')
    print(sample_name)
    with open('samples.txt', 'a') as f: print(sample_name, file=f)
    df['sample']=sample_name
    #print("filter:" + str(int(time.time()-timex)))
    # Convert data types as needed
    df['POS'] = pd.to_numeric(df['POS'])
    df.loc[df['QUAL']==".", 'QUAL']=0
    df['QUAL'] = pd.to_numeric(df['QUAL'])
    df=clean_dataframe(df)
    print("done:" + str(int(time.time()-timex)))
    #print(df)
    return df

    

def clean_dataframe(df):
    #Split to chrM and the rest
    #df_chrM=df[df['CHROM']=="chrM"]
    #df=df[df['CHROM']!="chrM"]
    #Sample column
    timex=time.time()
    x=df.iloc[:,7]
    x=x.str.split(":",expand=True,n=4)
    y = x.iloc[:,0]
    y[y=="1/1"]="Hom"
    y[y=="0/1"]="Het"
    y[y=="1|1"]="Hom"
    y[y=="0|1"]="Het"
    y[y=="1|0"]="Het"
    y[y=="1/0"]="Het"
    y[y=="0/0"]="Het"
    y[y=="1"]="Hem"
    df.loc[:,'ZYGOSITY']=y
    z= x=="DP"
    y = x.iloc[:,3]
    df.loc[:,'DP'] = y
    y = x.iloc[:,2]
    df.loc[:,'AF'] = y
    #chrM
    #x=df_chrM.iloc[:,7]
    #x=x.str.split(":",expand=True,n=7)
    #y = x.iloc[:,0]
    #y[y=="1/1"]="Hom"
    #y[y=="0/1"]="Het"
    #y[y=="1|1"]="Hom"
    #y[y=="0|1"]="Het"
    #y[y=="1|0"]="Het"
    #y[y=="1/0"]="Het"
    #y[y=="1"]="Hem"
    #df_chrM.loc[:,'ZYGOSITY']=y
    #z= x=="DP"
    #y = x.iloc[:,6]
    #df_chrM.loc[:,'DP'] = y
    #y = x.iloc[:,3]
    #df_chrM.loc[:,'AF'] = y
    #df=pd.concat([df, df_chrM], ignore_index=True)
    df['DP']=pd.to_numeric(df['DP'])
    df["AF"]=pd.to_numeric(df["AF"])
    df=df[["CHROM","POS","REF","ALT","QUAL","ZYGOSITY","DP","AF",'sample']]
    # Standard chromosomes data
    chroms=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21','22', 'X', 'Y']
    chroms= list(map(( lambda x: 'chr' + x), chroms) )
    df=df[df["CHROM"].isin(chroms)]
    # length of REF and ALT is 200 chr
    df['REF'] = df['REF'].str[:100]
    df['ALT'] = df['ALT'].str[:100]
    return df

def readAllVariants(table):
    query="SELECT variantID,uniqueID FROM " + table
    df= pd.read_sql(query, engine)
    return df

def writeSQL(df, table):
    df.to_sql(table,engine,if_exists='append',index=False,chunksize=1000000)


def import_file(vcf_file):
    all_vcf=pd.DataFrame()
    all_variants=readAllVariants('Variant')
    df = vcf_to_dataframe(vcf_file)
    df = df[~df["DP"].isna()]  #gene conversion removed e.g.  213262.norm.vcf.gz
    df['uniqueID']= df["CHROM"]+"_" +df["POS"].astype(str) +"_" +df["REF"]+"_" + df["ALT"]
    variant= df[["CHROM","POS","REF","ALT","uniqueID"]]
    vcf= df[["CHROM","POS","QUAL","ZYGOSITY","DP","AF","uniqueID","sample"]]
    # unique variant
    x= all_variants['uniqueID']
    y=vcf['uniqueID']
    #print('sample')
    #print(len(y))
    #print('all')
    #print(len(x))
    z=variant[~y.isin(x)]
    z.index = range(len(all_variants), len(all_variants) + len(z))
    writeSQL(z, "Variant")
    all_variants=readAllVariants('Variant')
    #print("variants : " + str(int(time.time()-timex)))
    vcf=pd.merge(vcf,all_variants, on='uniqueID', how='left')
    vcf["par"]= vcf["POS"]/5000000 
    vcf["par"]= vcf["par"].astype(int)
    vcf["partitionID"]= vcf["CHROM"]+"_"+vcf["par"].astype(str)
    vcf=vcf[["partitionID","QUAL","ZYGOSITY","DP","AF","uniqueID","sample","variantID"]]
    #vcf['variantID'].to_csv('variant_count.csv', mode='a',  index=False, header=False)
    writeSQL(vcf,"VCF_detection")




if __name__ == "__main__":
    vcf_file = main()
    timex=time.time()
    import_file(vcf_file)
    print("done : " + str(int(time.time()-timex)))


