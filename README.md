# 🧠 WellnessAI  
### AI-powered Wellness & Activity Recommendations  
Built by **The Couch Potatoes** — AI Hackathon Unboxed (PwC Greece)

---

## 🚀 Overview
**WellnessAI** είναι μια AI εφαρμογή που προτείνει προσωποποιημένες δραστηριότητες ευεξίας με βάση:

- Το **προφίλ** του χρήστη  
- Την **ανάλυση PDF εξετάσεων** (LangFlow)  
- Μια **fun AI κατηγοριοποίηση**  
- Τα **ενδιαφέροντα** και την **πόλη** του  

Ο κάθε χρήστης ταξινομείται σε μία από τις 4 κατηγορίες:
🥔 Couch Potato · 🙂 Just a Man/Woman · 🏃‍♂️ Athlete · 🦾 Iron Man

Από αυτή την κατηγορία + τα health data παράγονται προτάσεις δραστηριοτήτων.

---

## ✨ Key Features

### 🔐 Login / Registration  
Ασφαλής πρόσβαση & δημιουργία λογαριασμού.

### 👤 User Profile  
Εισαγωγή ηλικίας, πόλης, παθήσεων & ενδιαφερόντων.

### 📄 Medical PDF Upload  
AI parsing μέσω LangFlow → εξαγωγή βασικών ιατρικών τιμών.

### 🎭 Fun Categorization  
Το AI αναθέτει κατηγορία fitness (π.χ. Couch Potato → Athlete).

### 🎯 Event Recommendations  
Προσωποποιημένες προτάσεις δραστηριοτήτων σύμφωνα με:
- κατηγορία fitness  
- ενδιαφέροντα  
- επίπεδο υγείας  
- τοποθεσία  

### 📅 Calendar View  
Οι προτάσεις εμφανίζονται σε ημερολόγιο για εύκολη πλοήγηση.

---

## 🏛️ Architecture (Compact)

## 🛠️ Tech Stack

*   **Backend**: Python, Flask
*   **Database**: SQLite (with cloud persistence logic)
*   **AI & Machine Learning**:
    *   **Langflow**: Orchestration for PDF extraction and Event recommendation workflows.
    *   **XGBoost**: User classification model.
    *   **Scikit-Learn**: Data preprocessing and encoding.
*   **Data Processing**: Pandas, pdfplumber
*   **Frontend**: HTML5, CSS3, Bootstrap 5, JavaScript

  
*   Ολο το UI τρέχει μέσα στο AZURE.
## 📂 Project Structure

```
health-app/
├── app.py                      # Main Flask application (Routes & DB logic)
├── backend.py                  # API integration with Langflow & PDF processing
├── ml_service.py               # Machine Learning inference service (loads XGBoost model)
├── cluster_classify_algorithm.py # ML Training script (Generates model artifacts)
├── requirements.txt            # Python dependencies
├── users.db                    # SQLite database (local dev)
├── templates/                  # HTML Templates (Jinja2)
│   ├── home.html               # Dashboard with Cluster display
│   ├── profile.html            # User profile & PDF upload
│   ├── events.html             # Event calendar & recommendations
│   └── ...
├── static/
│   ├── css/                    # Custom styles
│   └── js/                     # Frontend scripts (Accessibility widget)
└── ...
```

## 🧠 How It Works

1.  **User Onboarding**: Users register and complete a basic profile (Age, City, Interests).
2.  **Data Ingestion**: Users can upload PDF lab results. The app parses these files (`backend.py`) to extract biomarker data.
3.  **Classification**: The `ml_service.py` loads a pre-trained XGBoost model to classify the user into a lifestyle cluster based on their data.
4.  **Event Recommendation**: The app communicates with a Langflow agent to fetch personalized event recommendations based on the user's location and interests.
5.  **Dashboard**: The user sees their "Cluster Level", progress, and a calendar of recommended events.

👥 Team

The Couch Potatoes:
Arsenis Chrysikopoulos,
Miltos Mourtias,
Manolis Papakyriakou,
Nikos Kafantaris,
Lefteris Tsimplekas,
## 📦 Installation & Setup

1.  Clone the repository.
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3.  Train-ready XGBoost model for classification.
4.  Run the Flask app:
    ```bash
    python app.py
    ```
5.  Visit `http://localhost:5000` in your browser.

   🏆 For

AI Hackathon Unboxed — PwC Greece 2025
