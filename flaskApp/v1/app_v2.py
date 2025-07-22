from flask import Flask, render_template, request
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

app = Flask(__name__)

# Database configuration
con_string = 'mysql+pymysql://root:Asdqwe123@localhost/vcf_sql'
engine = create_engine(con_string)
Session = sessionmaker(bind=engine)

@app.route('/', methods=['GET', 'POST'])
def index():
    results = []
    countries = []
    selected_country = None
    
    # Get list of countries for dropdown
    with Session() as session:
        try:
            query = text("""
                SELECT DISTINCT country
                FROM patient 
                ORDER BY country
            """)
            countries = [row[0] for row in session.execute(query).fetchall()]
        except Exception as e:
            print(f"Error fetching countries: {e}")
    
    # Process form submission
    if request.method == 'POST':
        selected_country = request.form.get('country')
        
        if selected_country:
            with Session() as session:
                try:
                    query = text("""
                        SELECT 
                            p.country,
                            a.SamplingMethod,
                            COUNT(*) as method_count
                        FROM 
                            analysis a
                        JOIN 
                            patient p ON a.SubjectID = p.SubjectID
                        WHERE 
                            p.country = :country
                        GROUP BY 
                            p.country, a.SamplingMethod
                        ORDER BY 
                            method_count DESC
                    """)
                    results = session.execute(query, {'country': selected_country}).fetchall()
                except Exception as e:
                    print(f"Error executing query: {e}")
    
    return render_template('index.html', 
                         countries=countries, 
                         results=results, 
                         selected_country=selected_country)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8028, debug=True)