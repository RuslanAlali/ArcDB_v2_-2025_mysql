
import pandas as pd
Arc2=pd.read_csv('results/merged_variant_data.csv')
Arc2['p']=(Arc2['count']/17).astype('int')
Arc2[Arc2['count']>168].to_csv('results_10p.csv', index=False)
print("0.10 of ArcDB2")


Arc1=pd.read_csv('arcad_raw_250509.csv')
Arc2=pd.read_csv('results_10p.csv')
print("read the two DB...")




Arc1['mark2']=Arc1['CHROM']+'_'+Arc1['POS'].astype('str')+Arc1['REF']+Arc1['ALT']
Arc2['mark2']=Arc2['Chrom']+'_'+Arc2['Pos'].astype('str')+Arc2['Ref']+Arc2['Alt']

x1=Arc1['mark2'].isin(Arc2['mark2']).value_counts()
print(x1)
x2=Arc2['mark2'].isin(Arc1['mark2']).value_counts()
print(x2)

# output validation
filename = "validation_output.txt"

with open(filename, 'w') as file:
    # Write x1, followed by a newline character
    file.write('Data in v1 vs v2')
    file.write(str(x1) + '\n')
    # Write x2, followed by a newline character
    file.write('Data in v2 vs v1')
    file.write(str(x2) + '\n')

print(f"Values of x1 and x2 written to '{filename}' on separate lines.")
