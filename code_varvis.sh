#!/bin/bash
input_file="/mnt/ruslan/ArcDB_v2/mysql_db/resource/vcf_file20250612.csv"
input_file="/mnt/ruslan/ArcDB_v2/mysql_db/resource/vcf_file_old_cegat_v2_annotation.csv"
input_file="/mnt/ruslan/ArcDB_v2/mysql_db/resource/vcf_file_ArcDragen_v3_annotation.csv"
input_file="/mnt/ruslan/ArcDB_v2/mysql_db/resource/vcf_file_old_varvis_v4_annotation"
#input_file="vcf.temp.csv"
if [ ! -f "$input_file" ]; then
    echo "Error: File $input_file not found"
    exit 1
fi
tail -n +2 "$input_file" | while IFS=, read -r orderid analysis runFolder snsv_vcf cnv_vcf sv_vcf bam; do
    # Remove any surrounding quotes from the orderid and paths
    orderid=$(echo "$orderid" | tr -d '"')
    snsv_vcf=$(echo "$snsv_vcf" | tr -d '"')
    
    # Define output filename
    output_vcf="/mnt/ruslan/ArcDB_v2/mysql_db/${orderid}_norm.vcf.gz"
    
    echo "Processing order ID: $orderid"
    echo "Input VCF: $snsv_vcf"
    echo "Output VCF: $output_vcf"
    # Execute bcftools norm command
    bcftools norm "$snsv_vcf" -m- any --threads 15 -Oz -o "$output_vcf"

    if [ $? -eq 0 ]; then
        echo "Successfully normalized $orderid"
    else
        echo "Error processing $orderid"
    fi

python add_vcf_2DB_GATK.py $output_vcf
rm $output_vcf 

    
done

echo "All samples processed"
