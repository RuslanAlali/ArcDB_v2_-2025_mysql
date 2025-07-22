from flask import Flask, render_template, request, make_response, url_for
from sqlalchemy import create_engine, text, MetaData, Table, select
from sqlalchemy.orm import sessionmaker
import pandas as pd
from collections import Counter
import io

app = Flask(__name__)

# Database connection string
# NOTE: Using 'root:Asdqwe123@localhost' is insecure for production environments.
# Consider using environment variables or a more secure method for credentials.
con_string = 'mysql+pymysql://root:Asdqwe123@localhost/vcf_sql'
engine = create_engine(con_string)

# Reflect database schema
metadata = MetaData()

# Define tables based on the assumed schema
# These tables will be used to construct SQLAlchemy queries
variant_table = Table('Variant', metadata, autoload_with=engine)
vcf_detection_table = Table('VCF_detection', metadata, autoload_with=engine)
patient_table = Table('patient', metadata, autoload_with=engine)
analysis_table = Table('analysis', metadata, autoload_with=engine)

# Create a session factory
Session = sessionmaker(bind=engine)

# Global variable to store data for CSV download (for demonstration purposes)
# In a real application, consider using Flask's session or a more robust caching mechanism
sample_country_data_for_csv = []

@app.route('/', methods=['GET', 'POST'])
def index():
    """
    Handles the main page, displaying the form and processing queries.
    """
    global sample_country_data_for_csv # Declare intent to modify global variable
    country_counts = None
    error_message = None
    csv_download_link = None
    sample_country_data_for_csv = [] # Clear previous data

    if request.method == 'POST':
        variant_input = request.form.get('variant_input')
        chrom = None
        pos_str = None
        pos = None
        if not variant_input:
            error_message = "Please enter a chromosome and position (e.g., chr1:12345)."
        else:
            parts = variant_input.split(':')
            if len(parts) == 2:
                chrom = parts[0].strip()
                pos_str = parts[1].strip()
                try:
                    pos = int(pos_str)
                    if pos < 0:
                        error_message = "Position must be a non-negative integer."
                except ValueError:
                    error_message = "Position must be a valid integer."
            else:
                error_message = "Invalid input format. Please use 'chromosome:position' (e.g., chr1:12345)."

        if not error_message:
            try:
                # Calculate partition
                partition_value = f"{chrom}_{str(int(pos / 5000000))}"

                # --- Query 1: Get variantID from 'variant' table ---
                # Select variantID where chrom and pos match
                stmt_variant = select(variant_table.c.variantID).where(
                    variant_table.c.Chrom == chrom,
                    variant_table.c.Pos == pos
                )
                
                with Session() as session:
                    # Execute the first query and fetch variant IDs
                    variant_ids_result = session.execute(stmt_variant).fetchall()
                    variant_ids = [row[0] for row in variant_ids_result]

                if not variant_ids:
                    error_message = f"No variant found for Chromosome: {chrom}, Position: {pos}"
                else:
                    # --- Query 2: Get SampleIDs from 'VCF_detection' table ---
                    # Select SampleID where variantID is in the list from Q1 and partition matches
                    stmt_vcf_detection = select(vcf_detection_table.c.sample).where(
                        vcf_detection_table.c.variantID.in_(variant_ids),
                        vcf_detection_table.c.partitionID == partition_value
                    )
                    
                    with Session() as session:
                        # Execute the second query and fetch sample IDs
                        sample_ids_result = session.execute(stmt_vcf_detection).fetchall()
                        sample_ids = [row[0] for row in sample_ids_result]

                    if not sample_ids:
                        error_message = f"No samples detected for the found variants and partition '{partition_value}'."
                    else:
                        # --- NEW Query 3: Get SubjectIDs and SampleIDs from 'analysis' table using SampleIDs ---
                        # Select SubjectID and SampleID where SampleID is in the list from Q2
                        stmt_analysis = select(
                            analysis_table.c.SubjectID,
                            analysis_table.c.SampleID
                        ).where(
                            analysis_table.c.SampleID.in_(sample_ids)
                        )

                        with Session() as session:
                            # Execute the third query and fetch subject IDs and sample IDs
                            analysis_results = session.execute(stmt_analysis).fetchall()
                            
                            # Create a dictionary to map SubjectID to SampleID
                            subject_to_sample_map = {row.SubjectID: row.SampleID for row in analysis_results}
                            subject_ids = list(subject_to_sample_map.keys())

                        if not subject_ids:
                            error_message = f"No subject IDs found for the detected samples."
                        else:
                            # --- Updated Query 4: Get countries from 'patient' table using SubjectIDs ---
                            # Select country and SubjectID for SubjectIDs found in NEW Q3
                            stmt_patient = select(
                                patient_table.c.country,
                                patient_table.c.SubjectID
                            ).where(
                                patient_table.c.SubjectID.in_(subject_ids)
                            )
                            
                            with Session() as session:
                                # Execute the fourth query and fetch countries and subject IDs
                                patient_results = session.execute(stmt_patient).fetchall()
                                
                                countries = []
                                for row in patient_results:
                                    country = row.country
                                    subject_id = row.SubjectID
                                    sample_id = subject_to_sample_map.get(subject_id)
                                    if sample_id:
                                        countries.append(country)
                                        sample_country_data_for_csv.append({
                                            'SampleID': sample_id,
                                            'Country': country
                                        })

                            # Count countries
                            if countries:
                                country_counts = Counter(countries)
                                csv_download_link = url_for('download_countries_csv')
                            else:
                                error_message = "No country information found for the detected subjects."


            except Exception as e:
                # Catch any database or processing errors
                error_message = f"An error occurred: {e}"
                print(f"Error: {e}") # Log the full error for debugging

    # Render the template with results or error message
    return render_template('country_counts_v3.html', 
                           country_counts=country_counts, 
                           error_message=error_message,
                           csv_download_link=csv_download_link)

@app.route('/download_countries_csv')
def download_countries_csv():
    """
    Generates and serves a CSV file containing SampleID and Country.
    """
    global sample_country_data_for_csv # Access the global variable

    if not sample_country_data_for_csv:
        return "No data to download. Please perform a search first.", 404

    # Create a Pandas DataFrame from the collected data
    df = pd.DataFrame(sample_country_data_for_csv)

    # Use io.StringIO to create an in-memory text buffer
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Get the value from the buffer
    csv_output = csv_buffer.getvalue()

    # Create a Flask response
    response = make_response(csv_output)
    response.headers["Content-Disposition"] = "attachment; filename=sample_country_data.csv"
    response.headers["Content-type"] = "text/csv"
    return response

if __name__ == '__main__':
    # Run the Flask app
    # For development, debug=True is useful, but should be False in production.
    app.run(host='0.0.0.0', port=8029, debug=True)