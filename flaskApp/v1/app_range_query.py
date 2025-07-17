from flask import Flask, request, jsonify, render_template
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

app = Flask(__name__)

# Database configuration
con_string = 'mysql+pymysql://root:Asdqwe123@localhost/vcf_sql'
engine = create_engine(con_string)
Session = sessionmaker(bind=engine)

@app.route('/')
def index():
    # Render the HTML page with the form
    chromosomes = [f'chr{i}' for i in range(1, 23)] + ['chrX', 'chrY', 'chrM']
    return render_template('index_pos_query.html', chromosomes=chromosomes)

@app.route('/api/variants', methods=['GET'])
def get_variants_with_detections():
    # Get query parameters
    chrom = request.args.get('chrom')
    start_pos = request.args.get('start_pos')
    end_pos = request.args.get('end_pos')
    
    # Validate required parameters
    if not all([chrom, start_pos, end_pos]):
        return jsonify({'error': 'Missing required parameters: chrom, start_pos, end_pos'}), 400
    
    # Validate chromosome
    valid_chromosomes = [f'chr{i}' for i in range(1, 23)] + ['chrX', 'chrY', 'chrM']
    if chrom not in valid_chromosomes:
        return jsonify({'error': f'Invalid chromosome. Must be one of: {", ".join(valid_chromosomes)}'}), 400
    
    # Validate positions are integers
    try:
        start_pos = int(start_pos)
        end_pos = int(end_pos)
    except ValueError:
        return jsonify({'error': 'start_pos and end_pos must be integers'}), 400
    
    # Validate position range
    if start_pos < 0 or end_pos < 0:
        return jsonify({'error': 'Positions must be positive integers'}), 400
    if start_pos > end_pos:
        return jsonify({'error': 'start_pos must be less than or equal to end_pos'}), 400
    
    session = Session()
    try:
        # Query 1: Get variants in the specified range
        variant_query = text("""
            SELECT variantID, Pos 
            FROM Variant 
            WHERE Chrom = :chrom AND Pos BETWEEN :start_pos AND :end_pos
        """)
        variants = session.execute(variant_query, {
            'chrom': chrom,
            'start_pos': start_pos,
            'end_pos': end_pos
        }).fetchall()
        
        if not variants:
            return jsonify({'message': 'No variants found in the specified range', 'results': []})

        
        # Prepare results dictionary
        results = []
        
        # Query 2: For each variant, get detection count
        detection_query = text("""
            SELECT COUNT(*) as detection_count 
            FROM VCF_detection 
            WHERE variantID = :variantID and  partitionID= :partition
        """)
        
        for variant in variants:
            variantID = variant.variantID
            variant_id = variant.variantID
            pos = variant.Pos
            partition = calculate_partition(chrom, pos)
            detection_count = session.execute(
                detection_query, 
                {'variantID': variantID,
                 'partition': partition}
            ).scalar()
            print(f"\nProcessing variant {variant_id} at position {pos} (partition: {partition})")
            results.append({
                'variant_ID': variant_id,
                'detection_count': detection_count
            })
        
        return jsonify({'results': results})
    
    except SQLAlchemyError as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500
    finally:
        session.close()

def calculate_partition(chrom, pos):
    partition_num = int(pos) // 5000000
    return f"{chrom}_{partition_num}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8029, debug=True)