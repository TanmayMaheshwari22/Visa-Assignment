# Visa-Assignment


# 🛠️ ETL Pipeline Setup Guide (MySQL → PostgreSQL)

## 📋 System Requirements

- **Python:** 3.8+
- **OS:** Ubuntu

---

## ⚙️ 1. Set Up Python Environment

### ✅ Step 1: Install Python & pip
```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv
```

### ✅ Step 2: Create a Virtual Environment
```bash
python3 -m venv myenv
```

### ✅ Step 3: Activate the Virtual Environment
```bash
source myenv/bin/activate
```

---

## 📦 2. Install Python Dependencies

Make sure you have the `requirements.txt` file in your project directory. Then run:
```bash
pip install -r requirements.txt
```

---

## 🐬 3. Install and Configure MySQL

### ✅ Step 1: Install MySQL Server
```bash
sudo apt install mysql-server
```

### ✅ Step 2: Retrieve Default Credentials
To get the root credentials used in the config file:
```bash
sudo cat /etc/mysql/debian.cnf
```

### ✅ Step 3: Login and Verify
```bash
mysql -u root -p
```
Use the password from the above command.

---

## 🐘 4. Install and Configure PostgreSQL

### ✅ Step 1: Install PostgreSQL
```bash
sudo apt install postgresql-16 postgresql-contrib-16
```

### ✅ Step 2: Start and Enable PostgreSQL
```bash
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

### ✅ Step 3: Login as PostgreSQL User
```bash
sudo -i -u postgres psql
```

### ✅ Step 4: Set a New Password
```sql
ALTER USER your_user WITH PASSWORD 'new_password';
```
Update this password in your `config.yaml` file.

---

## 🚀 5. Run the Scripts

Once **all dependencies are installed** and **passwords for MySQL and PostgreSQL are updated** in `config.yaml`, run:

```bash
# Step 1: Generate test data in MySQL
python3 records_creation.py

# Step 2: Run ETL and load to PostgreSQL
python3 cleaned_users.py
```

---

## 🗃️ 6. Accessing the Databases

### 🔍 Access Source DB (MySQL)
```bash
mysql -u root -p
```
Then:
```sql
SHOW DATABASES;
USE dev;
SELECT * FROM users;
```

### 🎯 Access Target DB (PostgreSQL)
```bash
sudo -i -u postgres psql
\c target
\dt
SELECT * FROM users_cleaned;
```

---

## 📄 Script Summaries

### Script 1: `records_creation.py`
- ✅ Creates a MySQL database `dev` and a `users` table.
- ✅ Inserts 1000 fake users with varied email formats (some invalid).
- ✅ Prepares the PostgreSQL target DB (`target`) for ETL.
- ✅ Reads credentials from `config.yaml`.

---

### Script 2: `cleaned_users.py`
- ✅ Reads connection configs.
- ✅ Extracts data from MySQL into a DataFrame.
- ✅ Transforms data:
  - Typecasting
  - Cleans malformed or missing emails
  - Lowercases all email entries
- ✅ Exports cleaned data to `cleaned_users.csv`.
- ✅ Loads the data into PostgreSQL `users_cleaned` table.

---

## 🔧 Email Cleaning Logic (Transformation Summary)

1. **Remove rows with missing emails**  
   ```python
   df = df[df['email'].notnull()]
   ```

2. **Filter out emails without '@'**  
   ```python
   df = df[df['email'].str.contains('@', na=False)]
   ```
   > This filters out clearly invalid emails by ensuring the presence of '@'.

3. **Remove emails ending with '@' (missing domain)**  
   ```python
   df = df[df['email'].str.strip().str[-1] != '@']
   ```

4. **Convert all emails to lowercase**  
   ```python
   df['email'] = df['email'].str.lower()
   ```
