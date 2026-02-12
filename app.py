import pandas as pd
import numpy as np
from flask import Flask, request, jsonify, send_file
from flask_sqlalchemy import SQLAlchemy
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
import os
import uuid
from datetime import datetime, timedelta
import jwt
from functools import wraps
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///datacleaner.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = 'your-secret-key'
app.config['UPLOAD_FOLDER'] = 'uploads/'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max

db = SQLAlchemy(app)
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# =========================
# Database Models
# =========================

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    subscription = db.Column(db.String(20), default='free')
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


# =========================
# Auth Decorator
# =========================

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')

        if not token:
            return jsonify({'error': 'Token missing'}), 401

        if token.startswith("Bearer "):
            token = token.split(" ")[1]

        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
            current_user = User.query.get(data['user_id'])

            if not current_user:
                return jsonify({'error': 'User not found'}), 401

        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401

        return f(current_user, *args, **kwargs)

    return decorated


# =========================
# Data Cleaning Class
# =========================

class DataCleaner:
    def __init__(self, df):
        self.df = df
        self.changes = []

    def analyze(self):
        return {
            'empty_cells': int(self.df.isnull().sum().sum()),
            'duplicate_rows': int(self.df.duplicated().sum()),
            'total_rows': len(self.df),
            'total_columns': len(self.df.columns)
        }

    def auto_fix(self):
        original_rows = len(self.df)

        # Remove duplicates
        duplicates = self.df.duplicated().sum()
        if duplicates > 0:
            self.df = self.df.drop_duplicates()
            self.changes.append(f"Removed {duplicates} duplicate rows")

        # Trim text spaces
        for col in self.df.select_dtypes(include=['object']).columns:
            self.df[col] = self.df[col].astype(str).str.strip()

        # Convert numeric text to numbers
        for col in self.df.select_dtypes(include=['object']).columns:
            converted = pd.to_numeric(self.df[col], errors='coerce')
            if converted.notna().sum() > len(self.df) * 0.5:
                self.df[col] = converted
                self.changes.append(f"Converted {col} to numeric")

        # Fill missing values
        for col in self.df.columns:
            if self.df[col].dtype in ['int64', 'float64']:
                self.df[col] = self.df[col].fillna(self.df[col].median())
            else:
                self.df[col] = self.df[col].fillna("Unknown")

        return {
            'rows_before': original_rows,
            'rows_after': len(self.df),
            'changes': self.changes
        }

    def get_cleaned_data(self):
        return self.df


# =========================
# Auth Routes
# =========================

@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()

    if User.query.filter_by(email=data['email']).first():
        return jsonify({'error': 'Email already exists'}), 400

    hashed_password = generate_password_hash(data['password'])

    new_user = User(
        email=data['email'],
        password=hashed_password
    )

    db.session.add(new_user)
    db.session.commit()

    token = jwt.encode({
        'user_id': new_user.id,
        'exp': datetime.utcnow() + timedelta(days=30)
    }, app.config['SECRET_KEY'], algorithm="HS256")

    if isinstance(token, bytes):
        token = token.decode('utf-8')

    return jsonify({'token': token, 'subscription': new_user.subscription})


@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    user = User.query.filter_by(email=data['email']).first()

    if not user or not check_password_hash(user.password, data['password']):
        return jsonify({'error': 'Invalid credentials'}), 401

    token = jwt.encode({
        'user_id': user.id,
        'exp': datetime.utcnow() + timedelta(days=30)
    }, app.config['SECRET_KEY'], algorithm="HS256")

    if isinstance(token, bytes):
        token = token.decode('utf-8')

    return jsonify({
        'token': token,
        'subscription': user.subscription,
        'files_cleaned': user.files_cleaned
    })


# =========================
# Upload Route
# =========================

@app.route('/upload', methods=['POST'])
@token_required
def upload_file(current_user):

    if current_user.subscription == 'free' and current_user.files_cleaned >= 5:
        return jsonify({'error': 'Free tier limit reached'}), 403

    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    if '.' not in file.filename:
        return jsonify({'error': 'Invalid filename'}), 400

    allowed = {'csv', 'xlsx', 'xls'}
    ext = file.filename.split('.')[-1].lower()

    if ext not in allowed:
        return jsonify({'error': 'Only CSV and Excel files allowed'}), 400

    filename = secure_filename(f"{uuid.uuid4()}_{file.filename}")
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)

    try:
        if ext == 'csv':
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
    except Exception as e:
        return jsonify({'error': str(e)}), 400

    cleaner = DataCleaner(df)
    analysis = cleaner.analyze()

    # Save temp CSV
    name, _ = os.path.splitext(filepath)
    temp_path = f"{name}_temp.csv"
    df.to_csv(temp_path, index=False)

    return jsonify({
        'file_id': filename,
        'analysis': analysis,
        'preview': df.head(5).to_dict('records')
    })


# =========================
# Clean Route
# =========================

@app.route('/clean', methods=['POST'])
@token_required
def clean_file(current_user):
    data = request.get_json()
    file_id = data.get('file_id')

    if not file_id:
        return jsonify({'error': 'File ID required'}), 400

    name, _ = os.path.splitext(os.path.join(app.config['UPLOAD_FOLDER'], file_id))
    temp_path = f"{name}_temp.csv"

    if not os.path.exists(temp_path):
        return jsonify({'error': 'File expired or not found'}), 404

    df = pd.read_csv(temp_path)

    cleaner = DataCleaner(df)
    analysis = cleaner.analyze()
    fix_report = cleaner.auto_fix()
    cleaned_df = cleaner.get_cleaned_data()

    cleaned_filename = f"cleaned_{file_id}"
    cleaned_path = os.path.join(app.config['UPLOAD_FOLDER'], cleaned_filename)

    cleaned_df.to_csv(cleaned_path, index=False)

    current_user.files_cleaned += 1
    db.session.commit()

    job = CleaningJob(
        user_id=current_user.id,
        original_filename=file_id,
        cleaned_filename=cleaned_filename,
        issues_found=analysis['empty_cells'] + analysis['duplicate_rows'],
        rows_cleaned=fix_report['rows_before'] - fix_report['rows_after']
    )

    db.session.add(job)
    db.session.commit()

    os.remove(temp_path)

    return jsonify({
        'success': True,
        'download_url': f'/download/{cleaned_filename}',
        'report': fix_report,
        'preview': cleaned_df.head(5).to_dict('records')
    })


# =========================
# Download Route
# =========================

@app.route('/download/<filename>')
@token_required
def download_file(current_user, filename):
    job = CleaningJob.query.filter_by(
        cleaned_filename=filename,
        user_id=current_user.id
    ).first()

    if not job:
        return jsonify({'error': 'Access denied'}), 404

    path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

    if not os.path.exists(path):
        return jsonify({'error': 'File not found'}), 404

    return send_file(path, as_attachment=True)


# =========================
# History Route
# =========================

@app.route('/history')
@token_required
def get_history(current_user):
    jobs = CleaningJob.query.filter_by(
        user_id=current_user.id
    ).order_by(CleaningJob.created_at.desc()).all()

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


# =========================
# Run App
# =========================

if __name__ == '__main__':
    with app.app_context():
        db.create_all()

    app.run(debug=True, port=5000)
