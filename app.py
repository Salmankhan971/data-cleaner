import pandas as pd
import numpy as np
from flask import Flask, request, jsonify, send_file
from flask_sqlalchemy import SQLAlchemy
from werkzeug.utils import secure_filename
import os
import uuid
from datetime import datetime
import jwt
from functools import wraps

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///datacleaner.db'
app.config['SECRET_KEY'] = 'your-secret-key'
app.config['UPLOAD_FOLDER'] = 'uploads/'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max

db = SQLAlchemy(app)
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Database Models
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password = db.Column(db.String(120), nullable=False)
    subscription = db.Column(db.String(20), default='free')  # free/pro/enterprise
    files_cleaned = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class CleaningJob(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    original_filename = db.Column(db.String(255))
    cleaned_filename = db.Column(db.String(255))
    issues_found = db.Column(db.Integer)
    rows_cleaned = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# Auth decorator
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'error': 'Token missing'}), 401
        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
            current_user = User.query.get(data['user_id'])
        except:
            return jsonify({'error': 'Invalid token'}), 401
        return f(current_user, *args, **kwargs)
    return decorated

# Data Cleaning Functions
class DataCleaner:
    def __init__(self, df):
        self.df = df
        self.issues = []
        self.changes = []
    
    def analyze(self):
        """Detect all issues in the dataset"""
        report = {
            'empty_cells': int(self.df.isnull().sum().sum()),
            'duplicate_rows': int(self.df.duplicated().sum()),
            'total_rows': len(self.df),
            'total_columns': len(self.df.columns),
            'column_issues': {}
        }
        
        for col in self.df.columns:
            col_issues = []
            
            # Check for mixed types
            if self.df[col].dtype == 'object':
                non_null = self.df[col].dropna()
                if len(non_null) > 0:
                    types = non_null.apply(lambda x: type(x).__name__).unique()
                    if len(types) > 1:
                        col_issues.append(f'Mixed types: {types}')
            
            # Check for leading/trailing spaces
            if self.df[col].dtype == 'object':
                spaces = self.df[col].astype(str).str.contains(r'^\s|\s$').sum()
                if spaces > 0:
                    col_issues.append(f'{spaces} values with extra spaces')
            
            # Check date formats
            if 'date' in col.lower() or 'time' in col.lower():
                try:
                    pd.to_datetime(self.df[col])
                except:
                    col_issues.append('Inconsistent date format')
            
            # Check numeric columns stored as text
            if self.df[col].dtype == 'object':
                numeric_count = pd.to_numeric(self.df[col], errors='coerce').notna().sum()
                if numeric_count > len(self.df) * 0.5:
                    col_issues.append(f'Numeric values stored as text ({numeric_count} detected)')
            
            if col_issues:
                report['column_issues'][col] = col_issues
        
        return report
    
    def auto_fix(self):
        """Apply automatic fixes"""
        original_shape = self.df.shape
        
        # 1. Remove duplicate rows
        duplicates = self.df.duplicated().sum()
        if duplicates > 0:
            self.df = self.df.drop_duplicates()
            self.changes.append(f'Removed {duplicates} duplicate rows')
        
        # 2. Trim spaces in text columns
        for col in self.df.select_dtypes(include=['object']).columns:
            if self.df[col].astype(str).str.contains(r'^\s|\s$').any():
                self.df[col] = self.df[col].astype(str).str.strip()
                self.changes.append(f'Trimmed spaces in column: {col}')
        
        # 3. Fix numeric columns stored as text
        for col in self.df.select_dtypes(include=['object']).columns:
            try:
                converted = pd.to_numeric(self.df[col], errors='coerce')
                if converted.notna().sum() > len(self.df) * 0.5:
                    self.df[col] = converted
                    self.changes.append(f'Converted {col} to numeric')
            except:
                pass
        
        # 4. Standardize date columns
        for col in self.df.columns:
            if 'date' in col.lower() or 'time' in col.lower():
                try:
                    self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                    self.changes.append(f'Standardized dates in {col}')
                except:
                    pass
        
        # 5. Fill empty cells
        empty_before = self.df.isnull().sum().sum()
        for col in self.df.columns:
            if self.df[col].dtype in ['int64', 'float64']:
                self.df[col] = self.df[col].fillna(self.df[col].median())
            else:
                self.df[col] = self.df[col].fillna('Unknown')
        
        if empty_before > 0:
            self.changes.append(f'Filled {empty_before} empty cells')
        
        return {
            'rows_before': original_shape[0],
            'rows_after': len(self.df),
            'changes': self.changes
        }
    
    def get_cleaned_data(self):
        return self.df

# API Routes

@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    if User.query.filter_by(email=data['email']).first():
        return jsonify({'error': 'Email exists'}), 400
    
    new_user = User(email=data['email'], password=data['password'])
    db.session.add(new_user)
    db.session.commit()
    
    token = jwt.encode({
        'user_id': new_user.id,
        'exp': datetime.utcnow() + datetime.timedelta(days=30)
    }, app.config['SECRET_KEY'])
    
    return jsonify({'token': token, 'subscription': new_user.subscription})

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    user = User.query.filter_by(email=data['email']).first()
    
    if not user or user.password != data['password']:
        return jsonify({'error': 'Invalid credentials'}), 401
    
    token = jwt.encode({
        'user_id': user.id,
        'exp': datetime.utcnow() + datetime.timedelta(days=30)
    }, app.config['SECRET_KEY'])
    
    return jsonify({
        'token': token,
        'subscription': user.subscription,
        'files_cleaned': user.files_cleaned
    })

@app.route('/upload', methods=['POST'])
@token_required
def upload_file(current_user):
    # Check limits
    if current_user.subscription == 'free' and current_user.files_cleaned >= 5:
        return jsonify({'error': 'Free tier: 5 files/month limit reached'}), 403
    
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    # Validate extension
    allowed = {'csv', 'xlsx', 'xls'}
    if not file.filename.split('.')[-1].lower() in allowed:
        return jsonify({'error': 'Only CSV and Excel files allowed'}), 400
    
    # Save file
    filename = secure_filename(f"{uuid.uuid4()}_{file.filename}")
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)
    
    # Read file
    try:
        if filename.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
    except Exception as e:
        return jsonify({'error': f'Could not read file: {str(e)}'}), 400
    
    # Analyze
    cleaner = DataCleaner(df)
    analysis = cleaner.analyze()
    
    # Save temp file for later processing
    temp_path = filepath.replace('.', '_temp.')
    df.to_csv(temp_path, index=False)
    
    return jsonify({
        'file_id': filename,
        'analysis': analysis,
        'preview': df.head(5).to_dict('records')
    })

@app.route('/clean', methods=['POST'])
@token_required
def clean_file(current_user):
    data = request.get_json()
    file_id = data.get('file_id')
    
    if not file_id:
        return jsonify({'error': 'File ID required'}), 400
    
    # Find temp file
    temp_path = os.path.join(app.config['UPLOAD_FOLDER'], file_id.replace('.', '_temp.'))
    if not os.path.exists(temp_path):
        return jsonify({'error': 'File expired or not found'}), 404
    
    # Read and clean
    df = pd.read_csv(temp_path)
    cleaner = DataCleaner(df)
    analysis = cleaner.analyze()
    fix_report = cleaner.auto_fix()
    cleaned_df = cleaner.get_cleaned_data()
    
    # Save cleaned file
    cleaned_filename = f"cleaned_{file_id.replace('uuid_', '')}"
    cleaned_path = os.path.join(app.config['UPLOAD_FOLDER'], cleaned_filename)
    
    if cleaned_filename.endswith('.csv'):
        cleaned_df.to_csv(cleaned_path, index=False)
    else:
        cleaned_df.to_excel(cleaned_path, index=False)
    
    # Update user stats
    current_user.files_cleaned += 1
    db.session.commit()
    
    # Save job record
    job = CleaningJob(
        user_id=current_user.id,
        original_filename=file_id,
        cleaned_filename=cleaned_filename,
        issues_found=analysis['empty_cells'] + analysis['duplicate_rows'],
        rows_cleaned=fix_report['rows_before'] - fix_report['rows_after']
    )
    db.session.add(job)
    db.session.commit()
    
    # Cleanup temp
    os.remove(temp_path)
    
    return jsonify({
        'success': True,
        'download_url': f'/download/{cleaned_filename}',
        'report': fix_report,
        'preview': cleaned_df.head(5).to_dict('records')
    })

@app.route('/download/<filename>')
@token_required
def download_file(current_user, filename):
    # Security: ensure user owns this file
    job = CleaningJob.query.filter_by(cleaned_filename=filename, user_id=current_user.id).first()
    if not job:
        return jsonify({'error': 'File not found or access denied'}), 404
    
    path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    if not os.path.exists(path):
        return jsonify({'error': 'File not found'}), 404
    
    return send_file(path, as_attachment=True)

@app.route('/history')
@token_required
def get_history(current_user):
    jobs = CleaningJob.query.filter_by(user_id=current_user.id).order_by(CleaningJob.created_at.desc()).all()
    return jsonify({
        'files_cleaned': current_user.files_cleaned,
        'subscription': current_user.subscription,
        'history': [{
            'id': job.id,
            'original': job.original_filename,
            'cleaned': job.cleaned_filename,
            'issues': job.issues_found,
            'rows_cleaned': job.rows_cleaned,
            'date': job.created_at.isoformat()
        } for job in jobs]
    })

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True, port=5000)
