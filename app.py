import os
import sqlite3
import hashlib
import datetime
import calendar
import json
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from backend import start_run_workflow, upload_pdf, upload_json, get_events

app = Flask(__name__)
app.secret_key = "supersecretkey"  # Replace with a secure key in production

# ---- DATABASE SETUP ----
# Azure Web Apps persistence logic
if "WEBSITE_SITE_NAME" in os.environ:
    # We are running on Azure
    # Use /home to persist data across deployments
    DEFAULT_DB_PATH = "/home/users.db"
else:
    # Local development
    DEFAULT_DB_PATH = "users.db"

DB_PATH = Path(os.environ.get("DB_PATH", DEFAULT_DB_PATH))

def get_connection():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row  # Access columns by name
    return conn

def init_db():
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL,
            password_hash TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS user_profiles (
            username TEXT PRIMARY KEY,
            age INTEGER,
            gender TEXT,
            condition_type TEXT,
            city TEXT,
            interests TEXT,
            FOREIGN KEY(username) REFERENCES users(username)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            name TEXT,
            address TEXT,
            zip TEXT,
            accessible TEXT,
            priceless TEXT,
            date TEXT,
            text TEXT,
            relevance_score REAL,
            FOREIGN KEY(username) REFERENCES users(username)
        )
    """)
    try:
        c.execute("ALTER TABLE user_profiles ADD COLUMN interests TEXT")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()

# Initialize DB on start
with app.app_context():
    init_db()

# ---- AUTH HELPERS ----
def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def register_user(username, first_name, last_name, email, password):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute('INSERT INTO users (username, first_name, last_name, email, password_hash) VALUES (?,?,?,?,?)', 
                  (username, first_name, last_name, email, hash_password(password)))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def check_login(username, password):
    conn = get_connection()
    c = conn.cursor()
    c.execute('SELECT password_hash FROM users WHERE username = ?', (username,))
    row = c.fetchone()
    conn.close()
    if row:
        stored_password_hash = row['password_hash']
        return stored_password_hash == hash_password(password)
    return False

# ---- DATA HELPERS ----
def save_user_profile(username, age, gender, condition_type, city, interests):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO user_profiles (username, age, gender, condition_type, city, interests)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(username) DO UPDATE SET
            age = excluded.age,
            gender = excluded.gender,
            condition_type = excluded.condition_type,
            city = excluded.city,
            interests = excluded.interests
        """,
        (username, age, gender, condition_type, city, interests),
    )
    conn.commit()
    conn.close()

def load_user_profile(username):
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT age, gender, condition_type, city, interests
        FROM user_profiles
        WHERE username = ?
        """,
        (username,),
    )
    row = c.fetchone()
    conn.close()
    if row:
        return dict(row)
    return None

def user_has_profile(username):
    profile = load_user_profile(username)
    if not profile:
        return False
    if not profile.get('age') or not profile.get('gender') or not profile.get('city'):
        return False
    return True

def save_events(username, events_response):
    if not events_response or "recommended_events" not in events_response:
        return
    
    conn = get_connection()
    c = conn.cursor()
    
    # Optional: Delete old events
    c.execute("DELETE FROM events WHERE username = ?", (username,))
    
    recommended_events = events_response.get("recommended_events", [])
    for event in recommended_events:
        c.execute("""
            INSERT INTO events (username, name, address, zip, accessible, priceless, date, text, relevance_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            username,
            event.get("name"),
            event.get("address"),
            event.get("zip"),
            event.get("accessible"),
            event.get("priceless"),
            event.get("date"),
            event.get("text"),
            event.get("relevance_score")
        ))
    conn.commit()
    conn.close()

def load_year_events(username, year):
    conn = get_connection()
    c = conn.cursor()
    start_date = datetime.date(year, 1, 1)
    end_date = datetime.date(year + 1, 1, 1)
    
    c.execute(
        """
        SELECT name, date, text
        FROM events
        WHERE username = ?
          AND date >= ?
          AND date < ?
        """,
        (username, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")),
    )
    rows = c.fetchall()
    conn.close()
    
    events_by_date = {}
    for row in rows:
        name = row['name']
        date_str = row['date']
        text = row['text']
        try:
            try:
                date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M").date()
            except ValueError:
                date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            continue
            
        date_str_key = date_obj.strftime("%Y-%m-%d")
        if date_str_key not in events_by_date:
            events_by_date[date_str_key] = []
        events_by_date[date_str_key].append({
            "name": name or "Event",
            "date": date_str_key,
            "description": text or ""
        })
    return events_by_date

def get_events_for_month(username, year, month):
    year_events = load_year_events(username, year)
    month_events = {}
    
    # Filter for month
    for date_str, events_list in year_events.items():
        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        if date_obj.year == year and date_obj.month == month:
            event_names = [e.get("name", "Event") for e in events_list]
            if event_names:
                month_events[date_str] = event_names
    return month_events


# ---- ROUTES ----

@app.route('/')
def index():
    if 'user' not in session:
        return redirect(url_for('login'))
    
    user = session['user']
    has_profile = user_has_profile(user)
    return render_template('home.html', user=user, has_profile=has_profile)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        if check_login(username, password):
            session['user'] = username
            if not user_has_profile(username):
                flash("Παρακαλώ συμπληρώστε το προφίλ σας για να δείτε τα αποτελέσματα!", "warning")
            else:
                flash(f"Καλώς ήρθες, {username}!", "success")
            return redirect(url_for('index'))
        else:
            flash("Λάθος στοιχεία σύνδεσης. Προσπάθησε ξανά.", "danger")
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        first_name = request.form.get('first_name')
        last_name = request.form.get('last_name')
        email = request.form.get('email')
        username = request.form.get('username')
        password = request.form.get('password')
        password2 = request.form.get('password2')
        
        if not all([first_name, last_name, email, username, password, password2]):
            flash("Παρακαλώ συμπληρώστε όλα τα πεδία.", "warning")
        elif password != password2:
            flash("Οι κωδικοί δεν ταιριάζουν.", "warning")
        else:
            ok = register_user(username, first_name, last_name, email, password)
            if ok:
                flash("Επιτυχής εγγραφή! Μπορείτε τώρα να συνδεθείτε.", "success")
                return redirect(url_for('login'))
            else:
                flash("Το όνομα χρήστη υπάρχει ήδη. Δοκιμάστε άλλο.", "danger")
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect(url_for('login'))

@app.route('/profile', methods=['GET', 'POST'])
def profile():
    if 'user' not in session:
        return redirect(url_for('login'))
    
    user = session['user']
    
    if request.method == 'POST':
        # Handle Profile Update
        if 'update_profile' in request.form:
            age = request.form.get('age')
            gender = request.form.get('gender')
            city = request.form.get('city')
            conditions = request.form.getlist('conditions')
            interests = request.form.getlist('interests')
            
            condition_type_str = ",".join(conditions) if conditions else None
            interests_str = ",".join(interests) if interests else None
            
            save_user_profile(user, age, gender, condition_type_str, city, interests_str)
            
            # Helper logic for backend integration
            json_data = {
                "username": user,
                "age": int(age) if age else None,
                "gender": gender,
                "condition_type": condition_type_str,
                "city": city,
                "interests": interests_str
            }
            
            try:
                response = upload_json(json_data, f"{user}.json")
                if response:
                    events = start_run_workflow(f"{user}.json", 'events', response)
                    if events:
                        save_events(user, events)
                        flash("Τα στοιχεία αποθηκεύτηκαν και ενημερώθηκαν τα events!", "success")
            except Exception as e:
                flash(f"Σφάλμα κατά την επικοινωνία με το backend: {str(e)}", "danger")
                
            return redirect(url_for('profile'))

        # Handle PDF Upload
        elif 'upload_pdf' in request.form:
            if 'pdf_file' not in request.files:
                flash("Δεν επιλέχθηκε αρχείο PDF.", "warning")
            else:
                file = request.files['pdf_file']
                if file.filename == '':
                    flash("Δεν επιλέχθηκε αρχείο PDF.", "warning")
                elif file:
                    try:
                        pdf_bytes = file.read()
                        response = upload_pdf(pdf_bytes, file.filename)
                        if response:
                            start_run_workflow(file.filename, 'injection', response)
                            flash("Το PDF αναλύθηκε επιτυχώς!", "success")
                    except Exception as e:
                        flash(f"Σφάλμα κατά την ανάλυση PDF: {str(e)}", "danger")
            return redirect(url_for('profile'))

    # Load existing profile
    existing = load_user_profile(user) or {}
    
    # Pre-process lists for templates
    existing['condition_list'] = existing.get('condition_type', '').split(',') if existing.get('condition_type') else []
    existing['interest_list'] = existing.get('interests', '').split(',') if existing.get('interests') else []
    
    return render_template('profile.html', user=user, profile=existing)

@app.route('/events')
def events():
    if 'user' not in session:
        return redirect(url_for('login'))
    
    user = session['user']
    today = datetime.date.today()
    
    try:
        year = int(request.args.get('year', today.year))
        month = int(request.args.get('month', today.month))
    except ValueError:
        year = today.year
        month = today.month

    # Load events
    month_events = get_events_for_month(user, year, month)
    
    # Calendar generation
    cal = calendar.Calendar(firstweekday=0)
    month_days = cal.monthdatescalendar(year, month)
    
    month_name = [
        "Ιανουάριος", "Φεβρουάριος", "Μάρτιος", "Απρίλιος",
        "Μάιος", "Ιούνιος", "Ιούλιος", "Αύγουστος",
        "Σεπτέμβριος", "Οκτώβριος", "Νοέμβριος", "Δεκέμβριος"
    ][month-1]

    return render_template('events.html', 
                           user=user, 
                           year=year, 
                           month=month, 
                           month_name=month_name,
                           month_days=month_days,
                           month_events=month_events,
                           today=today)

@app.route('/about')
def about():
    if 'user' not in session:
        return redirect(url_for('login'))
    return render_template('about.html', user=session['user'])

if __name__ == '__main__':
    app.run(debug=True)

