import os
import sqlite3
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import random
import string
import secrets
import hashlib
from functools import wraps
import json

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'default-dev-key-change-in-production')
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)
port = int(os.environ.get('PORT', 5000))

# Определяем тип БД: если есть DATABASE_URL – используем PostgreSQL, иначе SQLite
USE_POSTGRESQL = 'DATABASE_URL' in os.environ

# ==================== ФУНКЦИИ БАЗЫ ДАННЫХ ====================

def get_db_connection():
    """Возвращает соединение с БД (PostgreSQL на Render, SQLite локально)."""
    if USE_POSTGRESQL:
        conn = psycopg2.connect(os.environ['DATABASE_URL'], cursor_factory=RealDictCursor)
        return conn
    else:
        # Локально используем SQLite
        conn = sqlite3.connect('bank_system.db')
        conn.row_factory = sqlite3.Row
        return conn

def row_to_dict(row):
    """Преобразует строку результата (Row или RealDictRow) в словарь."""
    if row is None:
        return None
    return dict(row)

# ==================== ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ ====================

def init_db():
    """Создаёт все таблицы и заполняет начальными данными (совместимо с PostgreSQL и SQLite)."""
    conn = get_db_connection()
    cur = conn.cursor()

    # ----- Таблица пользователей -----
    if USE_POSTGRESQL:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                passport TEXT UNIQUE NOT NULL,
                full_name TEXT NOT NULL,
                account_number TEXT UNIQUE NOT NULL,
                balance REAL DEFAULT 0,
                is_active BOOLEAN DEFAULT TRUE,
                role_id INTEGER DEFAULT 6,
                password_hash TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    else:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                passport TEXT UNIQUE NOT NULL,
                full_name TEXT NOT NULL,
                account_number TEXT UNIQUE NOT NULL,
                balance REAL DEFAULT 0,
                is_active BOOLEAN DEFAULT 1,
                role_id INTEGER DEFAULT 6,
                password_hash TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

    # ----- Таблица ролей -----
    if USE_POSTGRESQL:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS roles (
                id SERIAL PRIMARY KEY,
                role_name TEXT UNIQUE NOT NULL,
                level INTEGER NOT NULL,
                permissions TEXT,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    else:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS roles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                role_name TEXT UNIQUE NOT NULL,
                level INTEGER NOT NULL,
                permissions TEXT,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

    # ----- Таблица транзакций -----
    if USE_POSTGRESQL:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                type TEXT NOT NULL,
                from_account TEXT NOT NULL,
                to_account TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                description TEXT,
                user_id INTEGER REFERENCES users(id) ON DELETE SET NULL
            )
        ''')
    else:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                type TEXT NOT NULL,
                from_account TEXT NOT NULL,
                to_account TEXT NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                description TEXT,
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        ''')

    # ----- Таблица аудита -----
    if USE_POSTGRESQL:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS audit_log (
                id SERIAL PRIMARY KEY,
                admin_passport TEXT NOT NULL,
                admin_name TEXT NOT NULL,
                action TEXT NOT NULL,
                target_user TEXT,
                details TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    else:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_passport TEXT NOT NULL,
                admin_name TEXT NOT NULL,
                action TEXT NOT NULL,
                target_user TEXT,
                details TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

    # ----- Таблица бизнесов -----
    if USE_POSTGRESQL:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS businesses (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                business_name TEXT NOT NULL,
                legal_name TEXT,
                tax_id TEXT UNIQUE,
                charter_capital REAL NOT NULL,
                address TEXT,
                email TEXT,
                phone TEXT,
                status TEXT DEFAULT 'pending',
                admin_notes TEXT,
                approved_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                approved_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    else:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS businesses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                business_name TEXT NOT NULL,
                legal_name TEXT,
                tax_id TEXT UNIQUE,
                charter_capital REAL NOT NULL,
                address TEXT,
                email TEXT,
                phone TEXT,
                status TEXT DEFAULT 'pending',
                admin_notes TEXT,
                approved_by INTEGER,
                approved_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id),
                FOREIGN KEY (approved_by) REFERENCES users (id)
            )
        ''')

    # ----- Таблица бизнес-счетов -----
    if USE_POSTGRESQL:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS business_accounts (
                id SERIAL PRIMARY KEY,
                business_id INTEGER NOT NULL REFERENCES businesses(id) ON DELETE CASCADE,
                account_number TEXT UNIQUE NOT NULL,
                account_type TEXT DEFAULT 'current',
                balance REAL DEFAULT 0,
                currency TEXT DEFAULT 'RUB',
                is_active BOOLEAN DEFAULT TRUE,
                credit_limit REAL DEFAULT 0,
                overdraft_allowed BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    else:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS business_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_id INTEGER NOT NULL,
                account_number TEXT UNIQUE NOT NULL,
                account_type TEXT DEFAULT 'current',
                balance REAL DEFAULT 0,
                currency TEXT DEFAULT 'RUB',
                is_active BOOLEAN DEFAULT 1,
                credit_limit REAL DEFAULT 0,
                overdraft_allowed BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (business_id) REFERENCES businesses (id)
            )
        ''')

    # ----- Таблица заявок на вывод средств -----
    if USE_POSTGRESQL:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS withdrawal_requests (
                id SERIAL PRIMARY KEY,
                business_account_id INTEGER NOT NULL REFERENCES business_accounts(id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                amount REAL NOT NULL,
                purpose TEXT NOT NULL,
                recipient_name TEXT,
                recipient_account TEXT,
                recipient_bank TEXT,
                status TEXT DEFAULT 'pending',
                admin_notes TEXT,
                processed_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                processed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    else:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS withdrawal_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_account_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                purpose TEXT NOT NULL,
                recipient_name TEXT,
                recipient_account TEXT,
                recipient_bank TEXT,
                status TEXT DEFAULT 'pending',
                admin_notes TEXT,
                processed_by INTEGER,
                processed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (business_account_id) REFERENCES business_accounts (id),
                FOREIGN KEY (user_id) REFERENCES users (id),
                FOREIGN KEY (processed_by) REFERENCES users (id)
            )
        ''')

    # ----- Таблица NFC-меток (привязка к обычным пользователям) -----
    if USE_POSTGRESQL:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS nfc_tags (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                tag_uid TEXT UNIQUE NOT NULL,
                tag_url TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    else:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS nfc_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                tag_uid TEXT UNIQUE NOT NULL,
                tag_url TEXT NOT NULL,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        ''')

    # ----- Таблица PIN-кодов -----
    if USE_POSTGRESQL:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS user_pins (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                nfc_tag_id INTEGER NOT NULL REFERENCES nfc_tags(id) ON DELETE CASCADE,
                pin_hash TEXT NOT NULL,
                pin_salt TEXT NOT NULL,
                attempts INTEGER DEFAULT 0,
                last_attempt TIMESTAMP,
                is_locked BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, nfc_tag_id)
            )
        ''')
    else:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS user_pins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                nfc_tag_id INTEGER NOT NULL,
                pin_hash TEXT NOT NULL,
                pin_salt TEXT NOT NULL,
                attempts INTEGER DEFAULT 0,
                last_attempt TIMESTAMP,
                is_locked BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id),
                FOREIGN KEY (nfc_tag_id) REFERENCES nfc_tags (id),
                UNIQUE(user_id, nfc_tag_id)
            )
        ''')

    # ----- Таблица сессий оплаты -----
    if USE_POSTGRESQL:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS payment_sessions (
                id SERIAL PRIMARY KEY,
                session_id TEXT UNIQUE NOT NULL,
                buyer_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                seller_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                amount REAL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                completed_at TIMESTAMP
            )
        ''')
    else:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS payment_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT UNIQUE NOT NULL,
                buyer_id INTEGER NOT NULL,
                seller_id INTEGER,
                amount REAL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY (buyer_id) REFERENCES users (id),
                FOREIGN KEY (seller_id) REFERENCES users (id)
            )
        ''')

    # ----- Индексы (для PostgreSQL синтаксис одинаков) -----
    cur.execute('CREATE INDEX IF NOT EXISTS idx_users_passport ON users(passport)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_users_account ON users(account_number)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_transactions_accounts ON transactions(from_account, to_account)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_nfc_tags_user ON nfc_tags(user_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_nfc_tags_uid ON nfc_tags(tag_uid)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_payment_sessions_session ON payment_sessions(session_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_businesses_user ON businesses(user_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_businesses_status ON businesses(status)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_business_accounts_business ON business_accounts(business_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_withdrawal_requests_status ON withdrawal_requests(status)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_user_pins_lookup ON user_pins(user_id, nfc_tag_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(timestamp)')

    # ----- Заполнение ролей -----
    default_roles = [
        (1, 'super_admin', 100, '{"all_permissions": true}', 'Главный администратор'),
        (2, 'special_admin', 90, '{"manage_users": true, "manage_nfc": true, "view_reports": true, "audit_logs": true}', 'Специальный администратор'),
        (3, 'admin', 80, '{"manage_users": true, "view_reports": true}', 'Администратор'),
        (4, 'digital_investigator', 70, '{"view_transactions": true, "view_reports": true, "audit_logs": true}', 'Цифровой следователь'),
        (5, 'passport_registrar', 60, '{"register_users": true, "view_users": true}', 'Регистратор паспортов'),
        (6, 'user', 10, '{"view_own_data": true, "make_payments": true}', 'Пользователь'),
        (7, 'business', 20, '{"view_own_data": true, "make_payments": true, "manage_business": true}', 'Бизнес-аккаунт')
    ]

    for role_id, role_name, level, permissions, description in default_roles:
        if USE_POSTGRESQL:
            cur.execute('''
                INSERT INTO roles (id, role_name, level, permissions, description)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            ''', (role_id, role_name, level, permissions, description))
        else:
            cur.execute('''
                INSERT OR IGNORE INTO roles (id, role_name, level, permissions, description)
                VALUES (?, ?, ?, ?, ?)
            ''', (role_id, role_name, level, permissions, description))

    # ----- Создание суперадмина (если нет) -----
    if USE_POSTGRESQL:
        cur.execute("SELECT * FROM users WHERE passport = 'admin001'")
    else:
        cur.execute("SELECT * FROM users WHERE passport = ?", ('admin001',))
    admin = cur.fetchone()
    if not admin:
        if USE_POSTGRESQL:
            cur.execute('''
                INSERT INTO users (passport, full_name, account_number, balance, role_id, password_hash, email, phone)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                'admin001',
                'Главный Администратор',
                'SUPER001',
                1000000,
                1,
                generate_password_hash('superadmin123'),
                'superadmin@bank.ru',
                '+79998887766'
            ))
        else:
            cur.execute('''
                INSERT INTO users (passport, full_name, account_number, balance, role_id, password_hash, email, phone)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                'admin001',
                'Главный Администратор',
                'SUPER001',
                1000000,
                1,
                generate_password_hash('superadmin123'),
                'superadmin@bank.ru',
                '+79998887766'
            ))
        print("✅ Создан суперадмин: admin001 / superadmin123")

    # ----- Тестовые пользователи (роль 6) -----
    test_users = [
        ('special001', 'Специальный Админ', 'SPEC001', 50000, 2, 'special123'),
        ('admin002', 'Обычный Админ', 'ADMIN002', 30000, 3, 'admin123'),
        ('invest001', 'Цифровой Следователь', 'INVEST001', 20000, 4, 'invest123'),
        ('regist001', 'Регистратор Паспортов', 'REGIST001', 15000, 5, 'regist123'),
        ('user002', 'Обычный Пользователь', 'USER002', 5000, 6, 'user123'),
        ('912312', 'КИЯМОВ КАРИМ МАРАТОВИЧ', f'ACC{random.randint(100000, 999999)}', 1000, 6, '123456')
    ]
    for passport, full_name, account_number, balance, role_id, password in test_users:
        if USE_POSTGRESQL:
            cur.execute("SELECT * FROM users WHERE passport = %s", (passport,))
        else:
            cur.execute("SELECT * FROM users WHERE passport = ?", (passport,))
        existing = cur.fetchone()
        if not existing:
            if USE_POSTGRESQL:
                cur.execute('''
                    INSERT INTO users (passport, full_name, account_number, balance, role_id, password_hash)
                    VALUES (%s, %s, %s, %s, %s, %s)
                ''', (passport, full_name, account_number, balance, role_id, generate_password_hash(password)))
            else:
                cur.execute('''
                    INSERT INTO users (passport, full_name, account_number, balance, role_id, password_hash)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (passport, full_name, account_number, balance, role_id, generate_password_hash(password)))
            print(f"✅ Создан тестовый пользователь: {passport} / {password}")

    # ----- Тестовый бизнес (для пользователя user002) -----
    if USE_POSTGRESQL:
        cur.execute("SELECT id FROM users WHERE passport = 'user002'")
    else:
        cur.execute("SELECT id FROM users WHERE passport = ?", ('user002',))
    user_row = cur.fetchone()
    if user_row:
        user_id = user_row['id']
        # Создаём бизнес
        if USE_POSTGRESQL:
            cur.execute('''
                INSERT INTO businesses (user_id, business_name, charter_capital, status, email, phone)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (tax_id) DO NOTHING
                RETURNING id
            ''', (user_id, 'Тестовый Бизнес ООО', 50000, 'approved', 'test_business@example.com', '+79990000001'))
            business_row = cur.fetchone()
            if business_row:
                business_id = business_row['id']
                cur.execute('''
                    INSERT INTO business_accounts (business_id, account_number, balance)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (account_number) DO NOTHING
                ''', (business_id, f'BUS{random.randint(100000, 999999)}', 50000))
        else:
            # SQLite – нужно получить lastrowid отдельно
            cur.execute('''
                INSERT OR IGNORE INTO businesses (user_id, business_name, charter_capital, status, email, phone)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, 'Тестовый Бизнес ООО', 50000, 'approved', 'test_business@example.com', '+79990000001'))
            # Если вставка произошла, получим id
            cur.execute("SELECT id FROM businesses WHERE user_id = ? AND business_name = 'Тестовый Бизнес ООО'", (user_id,))
            business_row = cur.fetchone()
            if business_row:
                business_id = business_row['id']
                cur.execute('''
                    INSERT OR IGNORE INTO business_accounts (business_id, account_number, balance)
                    VALUES (?, ?, ?)
                ''', (business_id, f'BUS{random.randint(100000, 999999)}', 50000))
        print("✅ Создан тестовый бизнес с балансом 50,000 ₽")

    conn.commit()
    cur.close()
    conn.close()
    print("✅ База данных инициализирована")

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def find_user_by_passport(passport):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT u.*, r.role_name, r.level, r.permissions 
            FROM users u
            LEFT JOIN roles r ON u.role_id = r.id
            WHERE u.passport = %s
        ''', (passport,))
    else:
        cur.execute('''
            SELECT u.*, r.role_name, r.level, r.permissions 
            FROM users u
            LEFT JOIN roles r ON u.role_id = r.id
            WHERE u.passport = ?
        ''', (passport,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return dict(user) if user else None

def find_user_by_account(account_number):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('SELECT * FROM users WHERE account_number = %s', (account_number,))
    else:
        cur.execute('SELECT * FROM users WHERE account_number = ?', (account_number,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return dict(user) if user else None

def find_user_by_id(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('SELECT * FROM users WHERE id = %s', (user_id,))
    else:
        cur.execute('SELECT * FROM users WHERE id = ?', (user_id,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return dict(user) if user else None

def get_user_role(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT r.* FROM roles r
            JOIN users u ON r.id = u.role_id
            WHERE u.id = %s
        ''', (user_id,))
    else:
        cur.execute('''
            SELECT r.* FROM roles r
            JOIN users u ON r.id = u.role_id
            WHERE u.id = ?
        ''', (user_id,))
    role = cur.fetchone()
    cur.close()
    conn.close()
    return dict(role) if role else None

def get_role_by_name(role_name):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('SELECT * FROM roles WHERE role_name = %s', (role_name,))
    else:
        cur.execute('SELECT * FROM roles WHERE role_name = ?', (role_name,))
    role = cur.fetchone()
    cur.close()
    conn.close()
    return dict(role) if role else None

def get_role_by_id(role_id):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('SELECT * FROM roles WHERE id = %s', (role_id,))
    else:
        cur.execute('SELECT * FROM roles WHERE id = ?', (role_id,))
    role = cur.fetchone()
    cur.close()
    conn.close()
    return dict(role) if role else None

def get_all_roles():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM roles ORDER BY level DESC')
    roles = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in roles]

def get_all_users():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT u.*, r.role_name,
               CASE WHEN u.role_id <= 3 THEN 1 ELSE 0 END as is_admin
        FROM users u
        LEFT JOIN roles r ON u.role_id = r.id
        ORDER BY u.created_at DESC
    ''')
    users = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(u) for u in users]

def update_user_balance(account_number, new_balance):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('UPDATE users SET balance = %s WHERE account_number = %s', (new_balance, account_number))
    else:
        cur.execute('UPDATE users SET balance = ? WHERE account_number = ?', (new_balance, account_number))
    conn.commit()
    cur.close()
    conn.close()

def add_transaction(transaction_type, from_account, to_account, amount, status, description, user_id=None):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('''
            INSERT INTO transactions (type, from_account, to_account, amount, status, description, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (transaction_type, from_account, to_account, amount, status, description, user_id))
    else:
        cur.execute('''
            INSERT INTO transactions (type, from_account, to_account, amount, status, description, user_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (transaction_type, from_account, to_account, amount, status, description, user_id))
    conn.commit()
    cur.close()
    conn.close()

def get_user_transactions(account_number, limit=10):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT * FROM transactions
            WHERE from_account = %s OR to_account = %s
            ORDER BY date DESC
            LIMIT %s
        ''', (account_number, account_number, limit))
    else:
        cur.execute('''
            SELECT * FROM transactions
            WHERE from_account = ? OR to_account = ?
            ORDER BY date DESC
            LIMIT ?
        ''', (account_number, account_number, limit))
    transactions = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(t) for t in transactions]

def check_permission(user_id, permission):
    role = get_user_role(user_id)
    if not role:
        return False
    if role['role_name'] == 'super_admin':
        return True
    try:
        permissions = json.loads(role['permissions'])
        if permissions.get('all_permissions'):
            return True
        return permissions.get(permission, False)
    except:
        return False

# ==================== EMAIL ФУНКЦИИ (без изменений) ====================
def send_email(to_email, subject, body, html_body=None):
    """Отправка email через SMTP (заглушка для отладки)."""
    try:
        print(f"\n{'='*60}")
        print(f"📧 EMAIL НА: {to_email}")
        print(f"📋 ТЕМА: {subject}")
        print(f"📝 СОДЕРЖИМОЕ:\n{body}")
        print(f"{'='*60}\n")
        return True
    except Exception as e:
        print(f"❌ Ошибка отправки email: {e}")
        return False

def send_business_approval_email(to_email, business_name, account_number, password, capital):
    subject = f"Ваша заявка на бизнес '{business_name}' одобрена"
    body = f"""
    Уважаемый владелец бизнеса,
    Ваша заявка на создание бизнеса "{business_name}" была одобрена.
    Данные для входа: логин BUS{account_number}, пароль {password}.
    Информация о счете: номер {account_number}, уставной капитал {capital} ₽.
    """
    return send_email(to_email, subject, body)

def send_business_rejection_email(to_email, business_name, reason):
    subject = f"Заявка на бизнес '{business_name}' отклонена"
    body = f"Уважаемый заявитель, ваша заявка на создание бизнеса \"{business_name}\" отклонена. Причина: {reason}"
    return send_email(to_email, subject, body)

def send_withdrawal_notification_email(to_email, amount, status, notes=None):
    status_text = "одобрена" if status == 'approved' else "отклонена"
    subject = f"Заявка на вывод средств {status_text}"
    body = f"Ваша заявка на вывод {amount} ₽ {status_text}. {notes or ''}"
    return send_email(to_email, subject, body)

# ==================== БИЗНЕС-ФУНКЦИИ (с поддержкой PostgreSQL RETURNING) ====================

def create_business_application(user_id, business_name, charter_capital, legal_name=None, tax_id=None,
                                address=None, email=None, phone=None):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if USE_POSTGRESQL:
            cur.execute('''
                INSERT INTO businesses (user_id, business_name, charter_capital, legal_name, tax_id,
                                        address, email, phone, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                RETURNING id
            ''', (user_id, business_name, charter_capital, legal_name, tax_id, address, email, phone))
            application_id = cur.fetchone()['id']
        else:
            cur.execute('''
                INSERT INTO businesses (user_id, business_name, charter_capital, legal_name, tax_id,
                                        address, email, phone, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            ''', (user_id, business_name, charter_capital, legal_name, tax_id, address, email, phone))
            application_id = cur.lastrowid
        conn.commit()
        # Логирование в аудит
        if USE_POSTGRESQL:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        else:
            cur.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        user = cur.fetchone()
        if USE_POSTGRESQL:
            cur.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (%s, %s, %s, %s, %s)
            ''', ('SYSTEM', 'Система', 'Подача заявки на бизнес',
                  user['passport'] if user else str(user_id),
                  f'Название: {business_name}, Уставной капитал: {charter_capital}'))
        else:
            cur.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (?, ?, ?, ?, ?)
            ''', ('SYSTEM', 'Система', 'Подача заявки на бизнес',
                  user['passport'] if user else str(user_id),
                  f'Название: {business_name}, Уставной капитал: {charter_capital}'))
        conn.commit()
        return application_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_business_applications(status=None):
    conn = get_db_connection()
    cur = conn.cursor()
    query = '''
        SELECT b.*, u.passport, u.full_name, u.email as user_email,
               a.full_name as approved_by_name
        FROM businesses b
        JOIN users u ON b.user_id = u.id
        LEFT JOIN users a ON b.approved_by = a.id
    '''
    params = []
    if status:
        query += ' WHERE b.status = %s' if USE_POSTGRESQL else ' WHERE b.status = ?'
        params.append(status)
    query += ' ORDER BY b.created_at DESC'
    if USE_POSTGRESQL:
        cur.execute(query, params)
    else:
        cur.execute(query, params)
    applications = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(app) for app in applications]

def get_business_by_id(business_id):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT b.*, u.passport, u.full_name, u.email as user_email
            FROM businesses b
            JOIN users u ON b.user_id = u.id
            WHERE b.id = %s
        ''', (business_id,))
    else:
        cur.execute('''
            SELECT b.*, u.passport, u.full_name, u.email as user_email
            FROM businesses b
            JOIN users u ON b.user_id = u.id
            WHERE b.id = ?
        ''', (business_id,))
    business = cur.fetchone()
    cur.close()
    conn.close()
    return dict(business) if business else None

def approve_business_application(business_id, admin_id, admin_notes=None):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Обновляем статус
        if USE_POSTGRESQL:
            cur.execute('''
                UPDATE businesses
                SET status = 'approved', approved_by = %s, approved_at = CURRENT_TIMESTAMP,
                    admin_notes = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            ''', (admin_id, admin_notes, business_id))
        else:
            cur.execute('''
                UPDATE businesses
                SET status = 'approved', approved_by = ?, approved_at = CURRENT_TIMESTAMP,
                    admin_notes = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (admin_id, admin_notes, business_id))

        # Получаем данные бизнеса
        if USE_POSTGRESQL:
            cur.execute('SELECT * FROM businesses WHERE id = %s', (business_id,))
        else:
            cur.execute('SELECT * FROM businesses WHERE id = ?', (business_id,))
        business = row_to_dict(cur.fetchone())

        # Создаём бизнес-счёт
        account_number = f'BUS{random.randint(100000, 999999)}'
        if USE_POSTGRESQL:
            cur.execute('''
                INSERT INTO business_accounts (business_id, account_number, balance)
                VALUES (%s, %s, %s)
            ''', (business_id, account_number, business['charter_capital']))
        else:
            cur.execute('''
                INSERT INTO business_accounts (business_id, account_number, balance)
                VALUES (?, ?, ?)
            ''', (business_id, account_number, business['charter_capital']))

        # Получаем данные пользователя
        if USE_POSTGRESQL:
            cur.execute('SELECT * FROM users WHERE id = %s', (business['user_id'],))
        else:
            cur.execute('SELECT * FROM users WHERE id = ?', (business['user_id'],))
        user = row_to_dict(cur.fetchone())

        # Генерируем пароль для бизнес-аккаунта
        business_password = ''.join(random.choices(string.ascii_letters + string.digits, k=10))

        # Создаём учётную запись бизнеса
        if USE_POSTGRESQL:
            cur.execute('''
                INSERT INTO users (passport, full_name, account_number, balance, role_id, password_hash, email, phone)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (
                f'BUS{business_id}',
                business['business_name'],
                account_number,
                business['charter_capital'],
                7,
                generate_password_hash(business_password),
                business.get('email') or user['email'],
                business.get('phone') or user['phone']
            ))
            business_user_id = cur.fetchone()['id']
        else:
            cur.execute('''
                INSERT INTO users (passport, full_name, account_number, balance, role_id, password_hash, email, phone)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                f'BUS{business_id}',
                business['business_name'],
                account_number,
                business['charter_capital'],
                7,
                generate_password_hash(business_password),
                business.get('email') or user['email'],
                business.get('phone') or user['phone']
            ))
            business_user_id = cur.lastrowid

        # Логируем в аудит
        if USE_POSTGRESQL:
            cur.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (%s, %s, %s, %s, %s)
            ''', ('SYSTEM', 'Система', 'Создание бизнес-аккаунта',
                  user['passport'],
                  f'Бизнес: {business["business_name"]}, Счет: {account_number}'))
        else:
            cur.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (?, ?, ?, ?, ?)
            ''', ('SYSTEM', 'Система', 'Создание бизнес-аккаунта',
                  user['passport'],
                  f'Бизнес: {business["business_name"]}, Счет: {account_number}'))

        conn.commit()

        email_to = business.get('email') or user.get('email')
        if email_to:
            send_business_approval_email(email_to, business['business_name'], account_number,
                                         business_password, business['charter_capital'])

        return {
            'business_id': business_id,
            'account_number': account_number,
            'password': business_password,
            'business_user_id': business_user_id
        }
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def reject_business_application(business_id, admin_id, admin_notes):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if USE_POSTGRESQL:
            cur.execute('''
                UPDATE businesses
                SET status = 'rejected', approved_by = %s, admin_notes = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            ''', (admin_id, admin_notes, business_id))
        else:
            cur.execute('''
                UPDATE businesses
                SET status = 'rejected', approved_by = ?, admin_notes = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (admin_id, admin_notes, business_id))

        # Логируем
        if USE_POSTGRESQL:
            cur.execute('SELECT * FROM businesses WHERE id = %s', (business_id,))
        else:
            cur.execute('SELECT * FROM businesses WHERE id = ?', (business_id,))
        business = row_to_dict(cur.fetchone())

        if business:
            if USE_POSTGRESQL:
                cur.execute('SELECT * FROM users WHERE id = %s', (business['user_id'],))
            else:
                cur.execute('SELECT * FROM users WHERE id = ?', (business['user_id'],))
            user = row_to_dict(cur.fetchone())

            if USE_POSTGRESQL:
                cur.execute('''
                    INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                    VALUES (%s, %s, %s, %s, %s)
                ''', ('SYSTEM', 'Система', 'Отклонение заявки на бизнес',
                      user['passport'] if user else str(business['user_id']),
                      f'Причина: {admin_notes}'))
            else:
                cur.execute('''
                    INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                    VALUES (?, ?, ?, ?, ?)
                ''', ('SYSTEM', 'Система', 'Отклонение заявки на бизнес',
                      user['passport'] if user else str(business['user_id']),
                      f'Причина: {admin_notes}'))

            email_to = business.get('email') or (user.get('email') if user else None)
            if email_to:
                send_business_rejection_email(email_to, business['business_name'], admin_notes)

        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def create_withdrawal_request(business_account_id, user_id, amount, purpose,
                              recipient_name=None, recipient_account=None, recipient_bank=None):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Проверяем баланс
        if USE_POSTGRESQL:
            cur.execute('SELECT * FROM business_accounts WHERE id = %s', (business_account_id,))
        else:
            cur.execute('SELECT * FROM business_accounts WHERE id = ?', (business_account_id,))
        account = cur.fetchone()
        if account['balance'] < amount:
            raise ValueError("Недостаточно средств на счете")

        if USE_POSTGRESQL:
            cur.execute('''
                INSERT INTO withdrawal_requests
                (business_account_id, user_id, amount, purpose, recipient_name, recipient_account, recipient_bank, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
                RETURNING id
            ''', (business_account_id, user_id, amount, purpose, recipient_name, recipient_account, recipient_bank))
            request_id = cur.fetchone()['id']
        else:
            cur.execute('''
                INSERT INTO withdrawal_requests
                (business_account_id, user_id, amount, purpose, recipient_name, recipient_account, recipient_bank, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
            ''', (business_account_id, user_id, amount, purpose, recipient_name, recipient_account, recipient_bank))
            request_id = cur.lastrowid

        # Логируем
        if USE_POSTGRESQL:
            cur.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (%s, %s, %s, %s, %s)
            ''', ('SYSTEM', 'Система', 'Заявка на вывод средств',
                  str(user_id),
                  f'Сумма: {amount}, Назначение: {purpose}'))
        else:
            cur.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (?, ?, ?, ?, ?)
            ''', ('SYSTEM', 'Система', 'Заявка на вывод средств',
                  str(user_id),
                  f'Сумма: {amount}, Назначение: {purpose}'))

        conn.commit()
        return request_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def get_withdrawal_requests(status=None):
    conn = get_db_connection()
    cur = conn.cursor()
    query = '''
        SELECT wr.*,
               ba.account_number,
               b.business_name,
               u.passport, u.full_name,
               p.full_name as processed_by_name
        FROM withdrawal_requests wr
        JOIN business_accounts ba ON wr.business_account_id = ba.id
        JOIN businesses b ON ba.business_id = b.id
        JOIN users u ON wr.user_id = u.id
        LEFT JOIN users p ON wr.processed_by = p.id
    '''
    params = []
    if status:
        query += ' WHERE wr.status = %s' if USE_POSTGRESQL else ' WHERE wr.status = ?'
        params.append(status)
    query += ' ORDER BY wr.created_at DESC'
    if USE_POSTGRESQL:
        cur.execute(query, params)
    else:
        cur.execute(query, params)
    requests = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(req) for req in requests]

def process_withdrawal_request(request_id, admin_id, status, admin_notes=None):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Получаем данные заявки
        if USE_POSTGRESQL:
            cur.execute('''
                SELECT wr.*, ba.balance, ba.account_number
                FROM withdrawal_requests wr
                JOIN business_accounts ba ON wr.business_account_id = ba.id
                WHERE wr.id = %s
            ''', (request_id,))
        else:
            cur.execute('''
                SELECT wr.*, ba.balance, ba.account_number
                FROM withdrawal_requests wr
                JOIN business_accounts ba ON wr.business_account_id = ba.id
                WHERE wr.id = ?
            ''', (request_id,))
        request = cur.fetchone()

        if not request:
            raise ValueError("Заявка не найдена")

        if request['status'] != 'pending':
            raise ValueError("Заявка уже обработана")

        if status == 'approved':
            if request['balance'] < request['amount']:
                raise ValueError("Недостаточно средств на счете")
            new_balance = request['balance'] - request['amount']
            if USE_POSTGRESQL:
                cur.execute('UPDATE business_accounts SET balance = %s WHERE id = %s',
                            (new_balance, request['business_account_id']))
            else:
                cur.execute('UPDATE business_accounts SET balance = ? WHERE id = ?',
                            (new_balance, request['business_account_id']))

            add_transaction('Вывод с бизнес-счета', request['account_number'], 'Банк',
                            request['amount'], 'Успешно', f'Вывод средств: {request["purpose"]}')

        # Обновляем статус заявки
        if USE_POSTGRESQL:
            cur.execute('''
                UPDATE withdrawal_requests
                SET status = %s, processed_by = %s, processed_at = CURRENT_TIMESTAMP, admin_notes = %s
                WHERE id = %s
            ''', (status, admin_id, admin_notes, request_id))
        else:
            cur.execute('''
                UPDATE withdrawal_requests
                SET status = ?, processed_by = ?, processed_at = CURRENT_TIMESTAMP, admin_notes = ?
                WHERE id = ?
            ''', (status, admin_id, admin_notes, request_id))

        # Логируем
        if USE_POSTGRESQL:
            cur.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (%s, %s, %s, %s, %s)
            ''', ('SYSTEM', 'Система', f'Обработка заявки на вывод: {status}',
                  str(request['user_id']),
                  f'Сумма: {request["amount"]}, Статус: {status}'))
        else:
            cur.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (?, ?, ?, ?, ?)
            ''', ('SYSTEM', 'Система', f'Обработка заявки на вывод: {status}',
                  str(request['user_id']),
                  f'Сумма: {request["amount"]}, Статус: {status}'))

        conn.commit()

        # Отправляем email
        if USE_POSTGRESQL:
            cur.execute('SELECT * FROM users WHERE id = %s', (request['user_id'],))
        else:
            cur.execute('SELECT * FROM users WHERE id = ?', (request['user_id'],))
        user = cur.fetchone()
        if user and user['email']:
            send_withdrawal_notification_email(user['email'], request['amount'], status, admin_notes)

        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

# ==================== NFC ФУНКЦИИ ====================

def create_pin_for_nfc(user_id, nfc_tag_id, pin):
    conn = get_db_connection()
    cur = conn.cursor()
    salt = secrets.token_hex(16)
    pin_hash = hashlib.sha256((pin + salt).encode()).hexdigest()
    if USE_POSTGRESQL:
        cur.execute('''
            INSERT INTO user_pins (user_id, nfc_tag_id, pin_hash, pin_salt)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, nfc_tag_id) DO UPDATE SET
                pin_hash = EXCLUDED.pin_hash,
                pin_salt = EXCLUDED.pin_salt,
                attempts = 0,
                is_locked = FALSE
        ''', (user_id, nfc_tag_id, pin_hash, salt))
    else:
        cur.execute('''
            INSERT OR REPLACE INTO user_pins (user_id, nfc_tag_id, pin_hash, pin_salt)
            VALUES (?, ?, ?, ?)
        ''', (user_id, nfc_tag_id, pin_hash, salt))
    conn.commit()
    cur.close()
    conn.close()
    return pin

def verify_pin(user_id, nfc_tag_id, pin):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT * FROM user_pins
            WHERE user_id = %s AND nfc_tag_id = %s AND is_locked = FALSE
        ''', (user_id, nfc_tag_id))
    else:
        cur.execute('''
            SELECT * FROM user_pins
            WHERE user_id = ? AND nfc_tag_id = ? AND is_locked = 0
        ''', (user_id, nfc_tag_id))
    pin_data = cur.fetchone()

    if not pin_data:
        cur.close()
        conn.close()
        return False

    if pin_data['attempts'] >= 5:
        if USE_POSTGRESQL:
            cur.execute('UPDATE user_pins SET is_locked = TRUE WHERE id = %s', (pin_data['id'],))
        else:
            cur.execute('UPDATE user_pins SET is_locked = 1 WHERE id = ?', (pin_data['id'],))
        conn.commit()
        cur.close()
        conn.close()
        return False

    salt = pin_data['pin_salt']
    pin_hash = hashlib.sha256((pin + salt).encode()).hexdigest()

    if pin_hash == pin_data['pin_hash']:
        if USE_POSTGRESQL:
            cur.execute('UPDATE user_pins SET attempts = 0 WHERE id = %s', (pin_data['id'],))
        else:
            cur.execute('UPDATE user_pins SET attempts = 0 WHERE id = ?', (pin_data['id'],))
        conn.commit()
        cur.close()
        conn.close()
        return True
    else:
        if USE_POSTGRESQL:
            cur.execute('''
                UPDATE user_pins
                SET attempts = attempts + 1, last_attempt = CURRENT_TIMESTAMP
                WHERE id = %s
            ''', (pin_data['id'],))
        else:
            cur.execute('''
                UPDATE user_pins
                SET attempts = attempts + 1, last_attempt = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (pin_data['id'],))
        conn.commit()
        cur.close()
        conn.close()
        return False

def generate_nfc_url(nfc_tag_id):
    unique_token = secrets.token_urlsafe(32)
    return f"/nfc/pay/{nfc_tag_id}/{unique_token}"

# ==================== ДЕКОРАТОРЫ (без изменений) ====================

def require_permission(permission):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('logged_in'):
                flash('Пожалуйста, войдите в систему', 'error')
                return redirect(url_for('index'))
            if session.get('role') == 'super_admin':
                return f(*args, **kwargs)
            try:
                permissions = json.loads(session.get('permissions', '{}'))
                if not permissions.get(permission, False):
                    flash('Доступ запрещен. Недостаточно прав.', 'error')
                    return redirect(url_for('dashboard'))
            except:
                flash('Ошибка проверки прав доступа', 'error')
                return redirect(url_for('dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def require_role(min_level):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('logged_in'):
                flash('Пожалуйста, войдите в систему', 'error')
                return redirect(url_for('index'))
            if session.get('role_level', 0) < min_level:
                flash('Доступ запрещен. Недостаточно прав.', 'error')
                return redirect(url_for('dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

# ==================== ИНИЦИАЛИЗАЦИЯ БД ПРИ СТАРТЕ ====================
with app.app_context():
    try:
        init_db()
        print("✅ База данных инициализирована при старте")
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")

# ==================== МАРШРУТЫ (с исправлениями для NFC) ====================

@app.route('/')
def index():
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def login():
    passport = request.form['passport']
    password = request.form['password']
    user = find_user_by_passport(passport)

    if user:
        if not user['is_active']:
            flash('Ваш аккаунт заблокирован. Обратитесь в поддержку.', 'error')
            return redirect(url_for('index'))
        if not check_password_hash(user['password_hash'], password):
            flash('Неверный номер паспорта или пароль', 'error')
            return redirect(url_for('index'))

        session['logged_in'] = True
        session['passport'] = passport
        session['user_id'] = user['id']
        session['user_info'] = user
        session['role'] = user['role_name']
        session['role_level'] = user['level']
        session['permissions'] = user['permissions']

        if user['role_name'] != 'user':
            return redirect(url_for('admin_panel'))
        else:
            return redirect(url_for('dashboard'))
    else:
        flash('Неверный номер паспорта или пароль', 'error')
        return redirect(url_for('index'))

@app.route('/dashboard')
def dashboard():
    if not session.get('logged_in'):
        return redirect(url_for('index'))
    user_info = session.get('user_info', {})
    transactions = get_user_transactions(user_info['account_number'], 10)
    return render_template('dashboard.html', user=user_info, transactions=transactions)

@app.route('/documents')
def documents():
    if not session.get('logged_in'):
        return redirect(url_for('index'))
    user_info = session.get('user_info', {})
    return render_template('documents.html', user=user_info)

@app.route('/change_password', methods=['GET', 'POST'])
def change_password():
    if not session.get('logged_in'):
        return redirect(url_for('index'))
    if request.method == 'POST':
        current_password = request.form['current_password']
        new_password = request.form['new_password']
        confirm_password = request.form['confirm_password']

        user = find_user_by_passport(session.get('passport'))

        if not check_password_hash(user['password_hash'], current_password):
            flash('Текущий пароль неверен', 'error')
            return render_template('change_password.html')

        if new_password != confirm_password:
            flash('Новые пароли не совпадают', 'error')
            return render_template('change_password.html')

        if len(new_password) < 6:
            flash('Пароль должен содержать минимум 6 символов', 'error')
            return render_template('change_password.html')

        conn = get_db_connection()
        cur = conn.cursor()
        if USE_POSTGRESQL:
            cur.execute('UPDATE users SET password_hash = %s WHERE passport = %s',
                        (generate_password_hash(new_password), session.get('passport')))
        else:
            cur.execute('UPDATE users SET password_hash = ? WHERE passport = ?',
                        (generate_password_hash(new_password), session.get('passport')))
        conn.commit()
        cur.close()
        conn.close()

        flash('Пароль успешно изменен', 'success')
        return redirect(url_for('dashboard'))

    return render_template('change_password.html')

@app.route('/transfer', methods=['POST'])
def transfer_money():
    if not session.get('logged_in'):
        return jsonify({'success': False, 'message': 'Не авторизован'})

    from_account = request.form['from_account']
    to_account = request.form['to_account']
    amount = float(request.form['amount'])
    description = request.form.get('description', '')

    from_user = find_user_by_account(from_account)
    to_user = find_user_by_account(to_account)

    if not from_user or not to_user:
        return jsonify({'success': False, 'message': 'Счет не найден'})

    if from_user['balance'] < amount:
        return jsonify({'success': False, 'message': 'Недостаточно средств'})

    if not from_user['is_active'] or not to_user['is_active']:
        return jsonify({'success': False, 'message': 'Счет заблокирован'})

    new_from_balance = from_user['balance'] - amount
    new_to_balance = to_user['balance'] + amount

    update_user_balance(from_account, new_from_balance)
    update_user_balance(to_account, new_to_balance)

    add_transaction('Перевод', from_account, to_account, amount, 'Успешно', description, from_user['id'])

    if session.get('passport') == from_user['passport']:
        session['user_info']['balance'] = new_from_balance

    return jsonify({
        'success': True,
        'message': f'Перевод на сумму {amount} руб. успешно выполнен',
        'new_balance': new_from_balance
    })

@app.route('/get_user_by_account/<account>')
def get_user_by_account(account):
    if not session.get('logged_in'):
        return jsonify({'error': 'Не авторизован'})
    user = find_user_by_account(account)
    if user:
        return jsonify({'name': user['full_name'], 'account': user['account_number']})
    else:
        return jsonify({'error': 'Пользователь не найден'})

# ==================== БИЗНЕС МАРШРУТЫ ====================

@app.route('/business/apply', methods=['GET', 'POST'])
def business_apply():
    if not session.get('logged_in'):
        return redirect(url_for('index'))
    if request.method == 'POST':
        try:
            business_name = request.form['business_name']
            charter_capital = float(request.form['charter_capital'])
            legal_name = request.form.get('legal_name', '')
            tax_id = request.form.get('tax_id', '')
            address = request.form.get('address', '')
            email = request.form.get('email', '')
            phone = request.form.get('phone', '')

            if charter_capital < 10000:
                flash('Минимальный уставной капитал - 10,000 ₽', 'error')
                return render_template('business_apply.html')

            application_id = create_business_application(
                session['user_id'], business_name, charter_capital,
                legal_name, tax_id, address, email, phone
            )
            flash('Заявка успешно отправлена! Ожидайте решения администратора.', 'success')
            return redirect(url_for('dashboard'))
        except Exception as e:
            flash(f'Ошибка при подаче заявки: {str(e)}', 'error')
    return render_template('business_apply.html')

@app.route('/admin/business_applications')
@require_permission('manage_users')
def admin_business_applications():
    status = request.args.get('status', 'pending')
    applications = get_business_applications(status)
    return render_template('admin_business_applications.html', applications=applications, status=status)

@app.route('/admin/business_applications/view/<int:business_id>')
@require_permission('manage_users')
def view_business_application(business_id):
    business = get_business_by_id(business_id)
    if not business:
        flash('Заявка не найдена', 'error')
        return redirect(url_for('admin_business_applications'))
    return render_template('business_application_view.html', business=business)

@app.route('/admin/business_applications/approve/<int:business_id>', methods=['POST'])
@require_permission('manage_users')
def approve_business_application_route(business_id):
    admin_notes = request.form.get('admin_notes', '')
    try:
        result = approve_business_application(business_id, session['user_id'], admin_notes)
        flash(f'Бизнес одобрен! Счет: {result["account_number"]}', 'success')
    except Exception as e:
        flash(f'Ошибка при одобрении: {str(e)}', 'error')
    return redirect(url_for('admin_business_applications'))

@app.route('/admin/business_applications/reject/<int:business_id>', methods=['POST'])
@require_permission('manage_users')
def reject_business_application_route(business_id):
    admin_notes = request.form.get('admin_notes', '')
    if not admin_notes:
        flash('Укажите причину отклонения', 'error')
        return redirect(url_for('view_business_application', business_id=business_id))
    try:
        reject_business_application(business_id, session['user_id'], admin_notes)
        flash('Заявка отклонена', 'success')
    except Exception as e:
        flash(f'Ошибка при отклонении: {str(e)}', 'error')
    return redirect(url_for('admin_business_applications'))

@app.route('/business/withdraw', methods=['GET', 'POST'])
def business_withdraw():
    if not session.get('logged_in'):
        return redirect(url_for('index'))
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT ba.*, b.business_name
            FROM business_accounts ba
            JOIN businesses b ON ba.business_id = b.id
            WHERE b.user_id = %s AND b.status = 'approved' AND ba.is_active = TRUE
        ''', (session['user_id'],))
    else:
        cur.execute('''
            SELECT ba.*, b.business_name
            FROM business_accounts ba
            JOIN businesses b ON ba.business_id = b.id
            WHERE b.user_id = ? AND b.status = 'approved' AND ba.is_active = 1
        ''', (session['user_id'],))
    business_accounts = cur.fetchall()
    cur.close()
    conn.close()

    if request.method == 'POST':
        try:
            business_account_id = int(request.form['business_account_id'])
            amount = float(request.form['amount'])
            purpose = request.form['purpose']
            recipient_name = request.form.get('recipient_name', '')
            recipient_account = request.form.get('recipient_account', '')
            recipient_bank = request.form.get('recipient_bank', '')

            if amount <= 0:
                flash('Введите корректную сумму', 'error')
                return render_template('business_withdraw.html', accounts=[dict(acc) for acc in business_accounts])

            request_id = create_withdrawal_request(
                business_account_id, session['user_id'], amount, purpose,
                recipient_name, recipient_account, recipient_bank
            )
            flash('Заявка на вывод средств отправлена на рассмотрение', 'success')
            return redirect(url_for('dashboard'))
        except ValueError as e:
            flash(str(e), 'error')
        except Exception as e:
            flash(f'Ошибка: {str(e)}', 'error')

    return render_template('business_withdraw.html', accounts=[dict(acc) for acc in business_accounts])

@app.route('/admin/withdrawal_requests')
@require_permission('manage_users')
def admin_withdrawal_requests():
    status = request.args.get('status', 'pending')
    requests = get_withdrawal_requests(status)
    return render_template('admin_withdrawal_requests.html', requests=requests, status=status)

@app.route('/admin/withdrawal_requests/view/<int:request_id>')
@require_permission('manage_users')
def view_withdrawal_request(request_id):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT wr.*,
                   ba.account_number,
                   b.business_name, b.legal_name, b.tax_id,
                   u.passport, u.full_name, u.email, u.phone,
                   p.full_name as processed_by_name
            FROM withdrawal_requests wr
            JOIN business_accounts ba ON wr.business_account_id = ba.id
            JOIN businesses b ON ba.business_id = b.id
            JOIN users u ON wr.user_id = u.id
            LEFT JOIN users p ON wr.processed_by = p.id
            WHERE wr.id = %s
        ''', (request_id,))
    else:
        cur.execute('''
            SELECT wr.*,
                   ba.account_number,
                   b.business_name, b.legal_name, b.tax_id,
                   u.passport, u.full_name, u.email, u.phone,
                   p.full_name as processed_by_name
            FROM withdrawal_requests wr
            JOIN business_accounts ba ON wr.business_account_id = ba.id
            JOIN businesses b ON ba.business_id = b.id
            JOIN users u ON wr.user_id = u.id
            LEFT JOIN users p ON wr.processed_by = p.id
            WHERE wr.id = ?
        ''', (request_id,))
    request_data = cur.fetchone()
    cur.close()
    conn.close()

    if not request_data:
        flash('Заявка не найдена', 'error')
        return redirect(url_for('admin_withdrawal_requests'))

    return render_template('withdrawal_request_view.html', request=dict(request_data))

@app.route('/admin/withdrawal_requests/process/<int:request_id>', methods=['POST'])
@require_permission('manage_users')
def process_withdrawal_request_route(request_id):
    action = request.form['action']
    admin_notes = request.form.get('admin_notes', '')
    status = 'approved' if action == 'approve' else 'rejected'
    try:
        process_withdrawal_request(request_id, session['user_id'], status, admin_notes)
        flash(f'Заявка {status}', 'success')
    except Exception as e:
        flash(f'Ошибка: {str(e)}', 'error')
    return redirect(url_for('admin_withdrawal_requests'))

@app.route('/admin/nfc', methods=['GET', 'POST'])
@require_permission('manage_nfc')
def admin_nfc():
    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'register':
            user_id = request.form.get('user_id')
            # fallback, если передали passport (для совместимости)
            if not user_id:
                passport = request.form.get('passport')
                if passport:
                    if USE_POSTGRESQL:
                        cur.execute("SELECT id FROM users WHERE passport = %s", (passport,))
                    else:
                        cur.execute("SELECT id FROM users WHERE passport = ?", (passport,))
                    user_row = cur.fetchone()
                    if user_row:
                        user_id = user_row['id']
            if not user_id:
                flash('Не выбран пользователь', 'error')
                return redirect(url_for('admin_nfc'))

            tag_uid = request.form.get('tag_uid', '').upper().strip()
            if not tag_uid:
                # генерируем случайный UID
                tag_uid = ''.join(random.choices('ABCDEF0123456789', k=16))
            pin_code = request.form.get('pin_code', '0000')

            # проверка уникальности UID
            if USE_POSTGRESQL:
                cur.execute("SELECT * FROM nfc_tags WHERE tag_uid = %s", (tag_uid,))
            else:
                cur.execute("SELECT * FROM nfc_tags WHERE tag_uid = ?", (tag_uid,))
            existing = cur.fetchone()
            if existing:
                flash('NFC-метка с таким UID уже зарегистрирована', 'error')
                return redirect(url_for('admin_nfc'))

            # создаём метку
            tag_url = f"/nfc/pay/0/temp"
            if USE_POSTGRESQL:
                cur.execute('''
                    INSERT INTO nfc_tags (user_id, tag_uid, tag_url)
                    VALUES (%s, %s, %s)
                    RETURNING id
                ''', (user_id, tag_uid, tag_url))
                nfc_tag_id = cur.fetchone()['id']
            else:
                cur.execute('''
                    INSERT INTO nfc_tags (user_id, tag_uid, tag_url)
                    VALUES (?, ?, ?)
                ''', (user_id, tag_uid, tag_url))
                nfc_tag_id = cur.lastrowid

            real_url = generate_nfc_url(nfc_tag_id)
            if USE_POSTGRESQL:
                cur.execute("UPDATE nfc_tags SET tag_url = %s WHERE id = %s", (real_url, nfc_tag_id))
            else:
                cur.execute("UPDATE nfc_tags SET tag_url = ? WHERE id = ?", (real_url, nfc_tag_id))

            create_pin_for_nfc(user_id, nfc_tag_id, pin_code)

            conn.commit()

            if USE_POSTGRESQL:
                cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            else:
                cur.execute("SELECT * FROM users WHERE id = ?", (user_id,))
            user = cur.fetchone()
            flash(f'NFC-метка зарегистрирована для пользователя {user["full_name"]}. PIN: {pin_code}', 'success')

    # Загружаем список пользователей для выпадающего списка (только role_id=6)
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT u.id, u.full_name, u.passport, u.account_number, r.role_name
            FROM users u
            JOIN roles r ON u.role_id = r.id
            WHERE u.is_active = TRUE AND u.role_id = 6
            ORDER BY u.full_name
        ''')
    else:
        cur.execute('''
            SELECT u.id, u.full_name, u.passport, u.account_number, r.role_name
            FROM users u
            JOIN roles r ON u.role_id = r.id
            WHERE u.is_active = 1 AND u.role_id = 6
            ORDER BY u.full_name
        ''')
    users = cur.fetchall()

    # Загружаем зарегистрированные NFC-метки
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT n.*, u.full_name, u.passport, u.account_number, r.role_name,
                   up.attempts, up.is_locked, up.last_attempt
            FROM nfc_tags n
            JOIN users u ON n.user_id = u.id
            JOIN roles r ON u.role_id = r.id
            LEFT JOIN user_pins up ON n.id = up.nfc_tag_id AND u.id = up.user_id
            ORDER BY n.created_at DESC
        ''')
    else:
        cur.execute('''
            SELECT n.*, u.full_name, u.passport, u.account_number, r.role_name,
                   up.attempts, up.is_locked, up.last_attempt
            FROM nfc_tags n
            JOIN users u ON n.user_id = u.id
            JOIN roles r ON u.role_id = r.id
            LEFT JOIN user_pins up ON n.id = up.nfc_tag_id AND u.id = up.user_id
            ORDER BY n.created_at DESC
        ''')
    nfc_tags = cur.fetchall()

    cur.close()
    conn.close()

    return render_template('admin_nfc.html',
                           nfc_tags=[dict(t) for t in nfc_tags],
                           users=[dict(u) for u in users])

@app.route('/nfc/pay/<int:nfc_tag_id>/<token>')
def nfc_payment_page(nfc_tag_id, token):
    if not session.get('logged_in'):
        flash('Пожалуйста, войдите в систему', 'error')
        return redirect(url_for('index'))

    conn = get_db_connection()
    cur = conn.cursor()

    # получаем данные NFC-метки
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT n.*, u.full_name, u.account_number, u.balance, u.email
            FROM nfc_tags n
            JOIN users u ON n.user_id = u.id
            WHERE n.id = %s AND n.is_active = TRUE
        ''', (nfc_tag_id,))
    else:
        cur.execute('''
            SELECT n.*, u.full_name, u.account_number, u.balance, u.email
            FROM nfc_tags n
            JOIN users u ON n.user_id = u.id
            WHERE n.id = ? AND n.is_active = 1
        ''', (nfc_tag_id,))
    nfc_tag = cur.fetchone()

    if not nfc_tag:
        cur.close()
        conn.close()
        return render_template('nfc_error.html', error="NFC-метка не найдена или заблокирована")

    # получаем продавца (текущий пользователь) – должен быть бизнесом
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT u.*, r.role_name
            FROM users u
            JOIN roles r ON u.role_id = r.id
            WHERE u.id = %s
        ''', (session['user_id'],))
    else:
        cur.execute('''
            SELECT u.*, r.role_name
            FROM users u
            JOIN roles r ON u.role_id = r.id
            WHERE u.id = ?
        ''', (session['user_id'],))
    seller = cur.fetchone()

    if seller['role_name'] != 'business':
        cur.close()
        conn.close()
        return render_template('nfc_error.html', error="Оплату могут принимать только бизнес-счета")

    if nfc_tag['user_id'] == seller['id']:
        cur.close()
        conn.close()
        return render_template('nfc_error.html', error="Вы не можете оплачивать сами себе")

    # создаём сессию оплаты
    session_id = secrets.token_urlsafe(32)
    expires_at = datetime.now() + timedelta(minutes=10)

    if USE_POSTGRESQL:
        cur.execute('''
            INSERT INTO payment_sessions (session_id, buyer_id, seller_id, expires_at)
            VALUES (%s, %s, %s, %s)
        ''', (session_id, nfc_tag['user_id'], seller['id'], expires_at))
    else:
        cur.execute('''
            INSERT INTO payment_sessions (session_id, buyer_id, seller_id, expires_at)
            VALUES (?, ?, ?, ?)
        ''', (session_id, nfc_tag['user_id'], seller['id'], expires_at))

    conn.commit()
    cur.close()
    conn.close()

    return render_template('nfc_payment.html',
                           buyer={
                               'full_name': nfc_tag['full_name'],
                               'account_number': nfc_tag['account_number'],
                               'balance': nfc_tag['balance']
                           },
                           seller={
                               'full_name': seller['full_name'],
                               'account_number': seller['account_number'],
                               'business_name': seller['full_name']
                           },
                           session_id=session_id)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

# ==================== АДМИН ПАНЕЛЬ ====================

@app.route('/admin')
@require_role(60)
def admin_panel():
    user_role = session.get('role')
    if user_role == 'super_admin':
        return render_template('admin_super.html')
    elif user_role == 'special_admin':
        return render_template('admin_special.html')
    elif user_role == 'admin':
        return render_template('admin_standard.html')
    elif user_role == 'digital_investigator':
        return render_template('admin_investigator.html')
    elif user_role == 'passport_registrar':
        return render_template('admin_registrar.html')
    else:
        flash('Доступ запрещен', 'error')
        return redirect(url_for('dashboard'))

@app.route('/admin/users')
@require_permission('manage_users')
def admin_users():
    users = get_all_users()
    return render_template('admin_users.html', users=users)

@app.route('/admin/transactions')
@require_permission('view_transactions')
def admin_transactions():
    conn = get_db_connection()
    cur = conn.cursor()
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    account = request.args.get('account', '')

    query = '''
        SELECT t.*, u1.full_name as from_name, u2.full_name as to_name
        FROM transactions t
        LEFT JOIN users u1 ON t.from_account = u1.account_number
        LEFT JOIN users u2 ON t.to_account = u2.account_number
        WHERE 1=1
    '''
    params = []
    if date_from:
        query += ' AND DATE(t.date) >= %s' if USE_POSTGRESQL else ' AND DATE(t.date) >= ?'
        params.append(date_from)
    if date_to:
        query += ' AND DATE(t.date) <= %s' if USE_POSTGRESQL else ' AND DATE(t.date) <= ?'
        params.append(date_to)
    if account:
        query += ' AND (t.from_account LIKE %s OR t.to_account LIKE %s)' if USE_POSTGRESQL else ' AND (t.from_account LIKE ? OR t.to_account LIKE ?)'
        params.append(f'%{account}%')
        params.append(f'%{account}%')
    query += ' ORDER BY t.date DESC LIMIT 100'
    if USE_POSTGRESQL:
        cur.execute(query, params)
    else:
        cur.execute(query, params)
    transactions = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('admin_transactions.html', transactions=[dict(t) for t in transactions])

@app.route('/admin/audit_logs')
@require_permission('audit_logs')
def admin_audit_logs():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT 100')
    logs = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('admin_audit.html', logs=[dict(log) for log in logs])

@app.route('/admin/system_settings')
@require_permission('all_permissions')
def admin_system_settings():
    return render_template('admin_system_settings.html')

@app.route('/admin/backup')
@require_permission('all_permissions')
def admin_backup():
    return render_template('admin_backup.html')

@app.route('/admin/register_nfc')
@require_permission('manage_nfc')
def admin_register_nfc():
    return redirect(url_for('admin_nfc'))

@app.route('/admin/add_user', methods=['GET', 'POST'])
@require_permission('manage_users')
def add_user():
    if request.method == 'GET':
        return render_template('admin_users.html')
    passport = request.form['passport']
    full_name = request.form['fio']
    account_number = request.form['account']
    balance = float(request.form['balance'])
    role_id = 3 if 'is_admin' in request.form else 6
    password = request.form['password']
    email = request.form.get('email', '')
    phone = request.form.get('phone', '')

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if USE_POSTGRESQL:
            cur.execute('''
                INSERT INTO users (passport, full_name, account_number, balance, role_id, password_hash, email, phone)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (passport, full_name, account_number, balance, role_id, generate_password_hash(password), email, phone))
            user_id = cur.fetchone()['id']
        else:
            cur.execute('''
                INSERT INTO users (passport, full_name, account_number, balance, role_id, password_hash, email, phone)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (passport, full_name, account_number, balance, role_id, generate_password_hash(password), email, phone))
            user_id = cur.lastrowid
        conn.commit()
        flash(f'Пользователь успешно добавлен (ID: {user_id})', 'success')
    except Exception as e:
        flash(f'Ошибка: пользователь с таким паспортом или номером счета уже существует', 'error')
    finally:
        cur.close()
        conn.close()
    return redirect(url_for('admin_users'))

@app.route('/admin/change_role/<passport>', methods=['GET', 'POST'])
@require_permission('manage_users')
def change_user_role(passport):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('SELECT * FROM users WHERE passport = %s', (passport,))
    else:
        cur.execute('SELECT * FROM users WHERE passport = ?', (passport,))
    user = cur.fetchone()
    if not user:
        flash('Пользователь не найден', 'error')
        return redirect(url_for('admin_users'))

    if USE_POSTGRESQL:
        cur.execute('SELECT * FROM roles WHERE id > 1 ORDER BY level DESC')
    else:
        cur.execute('SELECT * FROM roles WHERE id > 1 ORDER BY level DESC')
    roles = cur.fetchall()

    if request.method == 'POST':
        new_role_id = int(request.form['role_id'])
        if user['role_id'] == 1 and session.get('role') != 'super_admin':
            flash('Нельзя изменять роль суперадмина', 'error')
            return redirect(url_for('admin_users'))

        if USE_POSTGRESQL:
            cur.execute('UPDATE users SET role_id = %s WHERE passport = %s', (new_role_id, passport))
            cur.execute('SELECT * FROM roles WHERE id = %s', (new_role_id,))
        else:
            cur.execute('UPDATE users SET role_id = ? WHERE passport = ?', (new_role_id, passport))
            cur.execute('SELECT * FROM roles WHERE id = ?', (new_role_id,))
        new_role = cur.fetchone()

        if USE_POSTGRESQL:
            cur.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (%s, %s, %s, %s, %s)
            ''', (session.get('passport'),
                  session.get('user_info', {}).get('full_name', 'Администратор'),
                  'Изменение роли пользователя',
                  passport,
                  f'Новая роль: {new_role["role_name"] if new_role else "Неизвестно"}'))
        else:
            cur.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (?, ?, ?, ?, ?)
            ''', (session.get('passport'),
                  session.get('user_info', {}).get('full_name', 'Администратор'),
                  'Изменение роли пользователя',
                  passport,
                  f'Новая роль: {new_role["role_name"] if new_role else "Неизвестно"}'))
        conn.commit()
        flash(f'Роль пользователя {user["full_name"]} изменена на "{new_role["role_name"] if new_role else "Неизвестно"}"', 'success')
        return redirect(url_for('admin_users'))

    if USE_POSTGRESQL:
        cur.execute('SELECT * FROM roles WHERE id = %s', (user['role_id'],))
    else:
        cur.execute('SELECT * FROM roles WHERE id = ?', (user['role_id'],))
    current_role = cur.fetchone()

    cur.close()
    conn.close()

    return render_template('change_role.html',
                           user=dict(user),
                           current_role=dict(current_role) if current_role else None,
                           roles=[dict(r) for r in roles])

@app.route('/admin/toggle_block/<passport>')
@require_permission('manage_users')
def toggle_block_user(passport):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('SELECT * FROM users WHERE passport = %s', (passport,))
    else:
        cur.execute('SELECT * FROM users WHERE passport = ?', (passport,))
    user = cur.fetchone()
    if user:
        new_status = 0 if user['is_active'] else 1
        if USE_POSTGRESQL:
            cur.execute('UPDATE users SET is_active = %s WHERE passport = %s', (new_status, passport))
            cur.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (%s, %s, %s, %s, %s)
            ''', (session.get('passport'),
                  session.get('user_info', {}).get('full_name', 'Администратор'),
                  'Изменение статуса блокировки',
                  passport,
                  f'Новый статус: {"разблокирован" if new_status else "заблокирован"}'))
        else:
            cur.execute('UPDATE users SET is_active = ? WHERE passport = ?', (new_status, passport))
            cur.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (?, ?, ?, ?, ?)
            ''', (session.get('passport'),
                  session.get('user_info', {}).get('full_name', 'Администратор'),
                  'Изменение статуса блокировки',
                  passport,
                  f'Новый статус: {"разблокирован" if new_status else "заблокирован"}'))
        conn.commit()
        flash(f'Пользователь {passport} {"разблокирован" if new_status else "заблокирован"}', 'success')
    cur.close()
    conn.close()
    return redirect(url_for('admin_users'))

@app.route('/admin/toggle_admin/<passport>')
@require_permission('manage_users')
def toggle_admin_status_route(passport):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('SELECT * FROM users WHERE passport = %s', (passport,))
    else:
        cur.execute('SELECT * FROM users WHERE passport = ?', (passport,))
    user = cur.fetchone()
    if user:
        new_role_id = 6 if user['role_id'] <= 3 else 3
        if USE_POSTGRESQL:
            cur.execute('UPDATE users SET role_id = %s WHERE passport = %s', (new_role_id, passport))
        else:
            cur.execute('UPDATE users SET role_id = ? WHERE passport = ?', (new_role_id, passport))
        conn.commit()
        role_name = "администратором" if new_role_id <= 3 else "обычным пользователем"
        flash(f'Пользователь {passport} назначен {role_name}', 'success')
    cur.close()
    conn.close()
    return redirect(url_for('admin_users'))

@app.route('/admin/reset_password/<passport>')
@require_permission('manage_users')
def reset_password(passport):
    new_password = ''.join(random.choices(string.digits, k=8))
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('SELECT * FROM users WHERE passport = %s', (passport,))
    else:
        cur.execute('SELECT * FROM users WHERE passport = ?', (passport,))
    user = cur.fetchone()
    if user:
        if USE_POSTGRESQL:
            cur.execute('UPDATE users SET password_hash = %s WHERE passport = %s',
                        (generate_password_hash(new_password), passport))
        else:
            cur.execute('UPDATE users SET password_hash = ? WHERE passport = ?',
                        (generate_password_hash(new_password), passport))
        conn.commit()
        flash(f'Пароль для пользователя {passport} сброшен. Новый пароль: {new_password}', 'success')
    else:
        flash('Пользователь не найден', 'error')
    cur.close()
    conn.close()
    return redirect(url_for('admin_users'))

@app.route('/admin/add_money', methods=['POST'])
@require_permission('manage_users')
def add_money():
    account = request.form['account']
    amount = float(request.form['amount'])

    user = find_user_by_account(account)
    if user:
        new_balance = user['balance'] + amount
        update_user_balance(account, new_balance)
        add_transaction('Начисление', 'Система', account, amount, 'Успешно', 'Административное начисление', user['id'])
        flash(f'На счет {account} успешно начислено {amount} руб.', 'success')
    else:
        flash('Счет не найден', 'error')

    return redirect(url_for('admin_panel'))

@app.route('/admin/nfc/details/<int:nfc_id>')
@require_permission('manage_nfc')
def nfc_details(nfc_id):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT n.*, u.passport, u.full_name, u.account_number,
                   u.balance, u.email, u.phone, u.created_at as user_created
            FROM nfc_tags n
            JOIN users u ON n.user_id = u.id
            WHERE n.id = %s
        ''', (nfc_id,))
    else:
        cur.execute('''
            SELECT n.*, u.passport, u.full_name, u.account_number,
                   u.balance, u.email, u.phone, u.created_at as user_created
            FROM nfc_tags n
            JOIN users u ON n.user_id = u.id
            WHERE n.id = ?
        ''', (nfc_id,))
    nfc_tag = cur.fetchone()

    if not nfc_tag:
        flash('NFC-метка не найдена', 'error')
        return redirect(url_for('admin_nfc'))

    # транзакции
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT t.*,
                   u_from.full_name as from_name,
                   u_to.full_name as to_name,
                   CASE
                       WHEN t.from_account = u.account_number THEN 'outgoing'
                       ELSE 'incoming'
                   END as direction
            FROM transactions t
            JOIN users u ON (t.from_account = u.account_number OR t.to_account = u.account_number)
            LEFT JOIN users u_from ON t.from_account = u_from.account_number
            LEFT JOIN users u_to ON t.to_account = u_to.account_number
            WHERE u.id = %s
            ORDER BY t.date DESC
            LIMIT 50
        ''', (nfc_tag['user_id'],))
    else:
        cur.execute('''
            SELECT t.*,
                   u_from.full_name as from_name,
                   u_to.full_name as to_name,
                   CASE
                       WHEN t.from_account = u.account_number THEN 'outgoing'
                       ELSE 'incoming'
                   END as direction
            FROM transactions t
            JOIN users u ON (t.from_account = u.account_number OR t.to_account = u.account_number)
            LEFT JOIN users u_from ON t.from_account = u_from.account_number
            LEFT JOIN users u_to ON t.to_account = u_to.account_number
            WHERE u.id = ?
            ORDER BY t.date DESC
            LIMIT 50
        ''', (nfc_tag['user_id'],))
    transactions = cur.fetchall()

    # статистика
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT
                COUNT(*) as total_transactions,
                SUM(CASE WHEN t.from_account = u.account_number THEN t.amount ELSE 0 END) as total_sent,
                SUM(CASE WHEN t.to_account = u.account_number THEN t.amount ELSE 0 END) as total_received,
                MAX(t.date) as last_transaction
            FROM transactions t
            JOIN users u ON (t.from_account = u.account_number OR t.to_account = u.account_number)
            WHERE u.id = %s
        ''', (nfc_tag['user_id'],))
    else:
        cur.execute('''
            SELECT
                COUNT(*) as total_transactions,
                SUM(CASE WHEN t.from_account = u.account_number THEN t.amount ELSE 0 END) as total_sent,
                SUM(CASE WHEN t.to_account = u.account_number THEN t.amount ELSE 0 END) as total_received,
                MAX(t.date) as last_transaction
            FROM transactions t
            JOIN users u ON (t.from_account = u.account_number OR t.to_account = u.account_number)
            WHERE u.id = ?
        ''', (nfc_tag['user_id'],))
    stats = cur.fetchone()

    # информация о PIN
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT attempts, is_locked, last_attempt, created_at
            FROM user_pins
            WHERE nfc_tag_id = %s
        ''', (nfc_id,))
    else:
        cur.execute('''
            SELECT attempts, is_locked, last_attempt, created_at
            FROM user_pins
            WHERE nfc_tag_id = ?
        ''', (nfc_id,))
    pin_info = cur.fetchone()

    cur.close()
    conn.close()

    return render_template('nfc_details.html',
                           nfc_tag=dict(nfc_tag),
                           transactions=[dict(t) for t in transactions],
                           stats=dict(stats) if stats else {},
                           pin_info=dict(pin_info) if pin_info else {})

@app.route('/api/nfc/set_amount', methods=['POST'])
def set_payment_amount():
    if not request.is_json:
        return jsonify({'success': False, 'error': 'Неверный формат данных'})
    data = request.json
    session_id = data.get('session_id')
    amount = float(data.get('amount', 0))
    if amount <= 0:
        return jsonify({'success': False, 'error': 'Неверная сумма'})

    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT ps.*, s.account_number as seller_account
            FROM payment_sessions ps
            JOIN users s ON ps.seller_id = s.id
            WHERE ps.session_id = %s AND ps.status = 'pending'
        ''', (session_id,))
    else:
        cur.execute('''
            SELECT ps.*, s.account_number as seller_account
            FROM payment_sessions ps
            JOIN users s ON ps.seller_id = s.id
            WHERE ps.session_id = ? AND ps.status = 'pending'
        ''', (session_id,))
    session = cur.fetchone()
    if not session:
        cur.close()
        conn.close()
        return jsonify({'success': False, 'error': 'Сессия не найдена'})

    if USE_POSTGRESQL:
        cur.execute('UPDATE payment_sessions SET amount = %s WHERE session_id = %s', (amount, session_id))
    else:
        cur.execute('UPDATE payment_sessions SET amount = ? WHERE session_id = ?', (amount, session_id))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({'success': True, 'amount': amount})

@app.route('/api/nfc/confirm_payment', methods=['POST'])
def confirm_nfc_payment():
    if not request.is_json:
        return jsonify({'success': False, 'error': 'Неверный формат данных'})
    data = request.json
    session_id = data.get('session_id')
    pin = data.get('pin')

    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT ps.*,
                   b.account_number as buyer_account, b.balance as buyer_balance,
                   s.account_number as seller_account
            FROM payment_sessions ps
            JOIN users b ON ps.buyer_id = b.id
            JOIN users s ON ps.seller_id = s.id
            WHERE ps.session_id = %s
        ''', (session_id,))
    else:
        cur.execute('''
            SELECT ps.*,
                   b.account_number as buyer_account, b.balance as buyer_balance,
                   s.account_number as seller_account
            FROM payment_sessions ps
            JOIN users b ON ps.buyer_id = b.id
            JOIN users s ON ps.seller_id = s.id
            WHERE ps.session_id = ?
        ''', (session_id,))
    session = cur.fetchone()

    if not session:
        cur.close()
        conn.close()
        return jsonify({'success': False, 'error': 'Сессия не найдена'})

    # ищем NFC-метку покупателя
    if USE_POSTGRESQL:
        cur.execute('SELECT n.* FROM nfc_tags n WHERE n.user_id = %s LIMIT 1', (session['buyer_id'],))
    else:
        cur.execute('SELECT n.* FROM nfc_tags n WHERE n.user_id = ? LIMIT 1', (session['buyer_id'],))
    nfc_tag = cur.fetchone()

    if not nfc_tag:
        cur.close()
        conn.close()
        return jsonify({'success': False, 'error': 'NFC-метка не найдена'})

    if not verify_pin(session['buyer_id'], nfc_tag['id'], pin):
        cur.close()
        conn.close()
        return jsonify({'success': False, 'error': 'Неверный PIN-код'})

    amount = session['amount']
    if session['buyer_balance'] < amount:
        cur.close()
        conn.close()
        return jsonify({'success': False, 'error': 'Недостаточно средств'})

    new_buyer_balance = session['buyer_balance'] - amount
    if USE_POSTGRESQL:
        cur.execute('SELECT balance FROM users WHERE id = %s', (session['seller_id'],))
    else:
        cur.execute('SELECT balance FROM users WHERE id = ?', (session['seller_id'],))
    seller_row = cur.fetchone()
    new_seller_balance = seller_row['balance'] + amount

    if USE_POSTGRESQL:
        cur.execute('UPDATE users SET balance = %s WHERE id = %s', (new_buyer_balance, session['buyer_id']))
        cur.execute('UPDATE users SET balance = %s WHERE id = %s', (new_seller_balance, session['seller_id']))
        cur.execute('UPDATE payment_sessions SET status = %s, completed_at = CURRENT_TIMESTAMP WHERE session_id = %s',
                    ('paid', session_id))
    else:
        cur.execute('UPDATE users SET balance = ? WHERE id = ?', (new_buyer_balance, session['buyer_id']))
        cur.execute('UPDATE users SET balance = ? WHERE id = ?', (new_seller_balance, session['seller_id']))
        cur.execute('UPDATE payment_sessions SET status = ?, completed_at = CURRENT_TIMESTAMP WHERE session_id = ?',
                    ('paid', session_id))

    if USE_POSTGRESQL:
        cur.execute('''
            INSERT INTO transactions (type, from_account, to_account, amount, status, description)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', ('NFC Payment', session['buyer_account'], session['seller_account'],
              amount, 'Успешно', f'Оплата по NFC'))
    else:
        cur.execute('''
            INSERT INTO transactions (type, from_account, to_account, amount, status, description)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ('NFC Payment', session['buyer_account'], session['seller_account'],
              amount, 'Успешно', f'Оплата по NFC'))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        'success': True,
        'message': f'Оплата {amount} руб. прошла успешно',
        'new_balance': new_buyer_balance
    })

@app.route('/api/nfc/status/<session_id>')
def get_payment_status(session_id):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('SELECT status, amount FROM payment_sessions WHERE session_id = %s', (session_id,))
    else:
        cur.execute('SELECT status, amount FROM payment_sessions WHERE session_id = ?', (session_id,))
    session = cur.fetchone()
    cur.close()
    conn.close()
    if session:
        return jsonify(dict(session))
    return jsonify({'status': 'not_found'})

# ==================== API ДЛЯ АДМИНОВ ====================

@app.route('/admin/api/system_stats')
@require_permission('view_reports')
def admin_system_stats():
    conn = get_db_connection()
    cur = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    if USE_POSTGRESQL:
        cur.execute('SELECT COUNT(*) as count FROM transactions WHERE DATE(date) = %s', (today,))
    else:
        cur.execute('SELECT COUNT(*) as count FROM transactions WHERE DATE(date) = ?', (today,))
    today_transactions = cur.fetchone()['count']

    if USE_POSTGRESQL:
        cur.execute('SELECT AVG(balance) as avg FROM users WHERE is_active = TRUE')
    else:
        cur.execute('SELECT AVG(balance) as avg FROM users WHERE is_active = 1')
    avg_result = cur.fetchone()
    avg_balance = avg_result['avg'] if avg_result['avg'] is not None else 0

    if USE_POSTGRESQL:
        cur.execute('SELECT COALESCE(SUM(amount), 0) as total FROM transactions WHERE DATE(date) = %s AND status = %s',
                    (today, 'Успешно'))
    else:
        cur.execute('SELECT COALESCE(SUM(amount), 0) as total FROM transactions WHERE DATE(date) = ? AND status = ?',
                    (today, 'Успешно'))
    turnover_result = cur.fetchone()
    total_turnover = turnover_result['total'] if turnover_result['total'] is not None else 0

    if USE_POSTGRESQL:
        cur.execute('SELECT COUNT(DISTINCT user_id) as count FROM transactions WHERE DATE(date) = %s', (today,))
    else:
        cur.execute('SELECT COUNT(DISTINCT user_id) as count FROM transactions WHERE DATE(date) = ?', (today,))
    active_today = cur.fetchone()['count']

    if USE_POSTGRESQL:
        cur.execute('SELECT COUNT(*) as count FROM users WHERE DATE(created_at) = %s', (today,))
    else:
        cur.execute('SELECT COUNT(*) as count FROM users WHERE DATE(created_at) = ?', (today,))
    new_today = cur.fetchone()['count']

    cur.close()
    conn.close()

    return jsonify({
        'today_transactions': today_transactions,
        'avg_balance': round(float(avg_balance), 2),
        'total_turnover': round(float(total_turnover), 2),
        'active_today': active_today,
        'new_today': new_today
    })

@app.route('/admin/api/super_stats')
@require_permission('all_permissions')
def api_super_stats():
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('SELECT COUNT(*) as count FROM users')
    else:
        cur.execute('SELECT COUNT(*) as count FROM users')
    total_users = cur.fetchone()['count']

    if USE_POSTGRESQL:
        cur.execute('SELECT COUNT(*) as count FROM payment_sessions WHERE status = %s', ('pending',))
    else:
        cur.execute('SELECT COUNT(*) as count FROM payment_sessions WHERE status = ?', ('pending',))
    active_sessions = cur.fetchone()['count']

    today = datetime.now().strftime('%Y-%m-%d')
    if USE_POSTGRESQL:
        cur.execute('SELECT COUNT(*) as count FROM transactions WHERE DATE(date) = %s', (today,))
    else:
        cur.execute('SELECT COUNT(*) as count FROM transactions WHERE DATE(date) = ?', (today,))
    today_transactions = cur.fetchone()['count']

    if USE_POSTGRESQL:
        cur.execute('SELECT SUM(balance) as total FROM users')
    else:
        cur.execute('SELECT SUM(balance) as total FROM users')
    total_balance = cur.fetchone()['total'] or 0

    cur.close()
    conn.close()

    return jsonify({
        'total_users': total_users,
        'active_sessions': active_sessions,
        'today_transactions': today_transactions,
        'total_balance': round(total_balance, 2)
    })

@app.route('/admin/api/analyze_transactions', methods=['POST'])
@require_permission('view_transactions')
def api_analyze_transactions():
    data = request.json
    conn = get_db_connection()
    cur = conn.cursor()

    query = 'SELECT * FROM transactions WHERE 1=1'
    params = []
    if data.get('date_from'):
        query += ' AND DATE(date) >= %s' if USE_POSTGRESQL else ' AND DATE(date) >= ?'
        params.append(data['date_from'])
    if data.get('date_to'):
        query += ' AND DATE(date) <= %s' if USE_POSTGRESQL else ' AND DATE(date) <= ?'
        params.append(data['date_to'])
    if data.get('min_amount'):
        query += ' AND amount >= %s' if USE_POSTGRESQL else ' AND amount >= ?'
        params.append(float(data['min_amount']))
    if data.get('max_amount'):
        query += ' AND amount <= %s' if USE_POSTGRESQL else ' AND amount <= ?'
        params.append(float(data['max_amount']))
    query += ' ORDER BY date DESC LIMIT 100'

    if USE_POSTGRESQL:
        cur.execute(query, params)
    else:
        cur.execute(query, params)
    transactions = cur.fetchall()

    total_amount = sum(t['amount'] for t in transactions)
    average_amount = total_amount / len(transactions) if transactions else 0

    cur.close()
    conn.close()

    return jsonify({
        'summary': {
            'count': len(transactions),
            'total_amount': round(total_amount, 2),
            'average_amount': round(average_amount, 2)
        },
        'transactions': [dict(t) for t in transactions]
    })

@app.route('/admin/api/recent_registrations')
@require_permission('view_users')
def api_recent_registrations():
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT passport, full_name, created_at, balance
            FROM users
            WHERE role_id = 6
            ORDER BY created_at DESC
            LIMIT 20
        ''')
    else:
        cur.execute('''
            SELECT passport, full_name, created_at, balance
            FROM users
            WHERE role_id = 6
            ORDER BY created_at DESC
            LIMIT 20
        ''')
    users = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([dict(u) for u in users])

@app.route('/admin/api/admin_logs')
@require_permission('audit_logs')
def admin_admin_logs():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT 50')
    logs = cur.fetchall()
    cur.close()
    conn.close()
    result = []
    for log in logs:
        result.append({
            'id': log['id'],
            'admin_name': log['admin_name'],
            'action': log['action'],
            'target_user': log['target_user'],
            'details': log['details'],
            'timestamp': log['timestamp']
        })
    return jsonify(result)

@app.route('/admin/api/search_users')
@require_permission('view_users')
def admin_search_users():
    query = request.args.get('q', '')
    if not query:
        return jsonify([])

    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('''
            SELECT id, passport, full_name, account_number, balance, is_active,
                   CASE WHEN role_id <= 3 THEN 1 ELSE 0 END as is_admin
            FROM users
            WHERE passport ILIKE %s OR full_name ILIKE %s OR account_number ILIKE %s
            LIMIT 20
        ''', (f'%{query}%', f'%{query}%', f'%{query}%'))
    else:
        cur.execute('''
            SELECT id, passport, full_name, account_number, balance, is_active,
                   CASE WHEN role_id <= 3 THEN 1 ELSE 0 END as is_admin
            FROM users
            WHERE passport LIKE ? OR full_name LIKE ? OR account_number LIKE ?
            LIMIT 20
        ''', (f'%{query}%', f'%{query}%', f'%{query}%'))
    users = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([dict(u) for u in users])

@app.route('/admin/api/user_transactions/<passport>')
@require_permission('view_transactions')
def admin_user_transactions(passport):
    conn = get_db_connection()
    cur = conn.cursor()
    if USE_POSTGRESQL:
        cur.execute('SELECT * FROM users WHERE passport = %s', (passport,))
    else:
        cur.execute('SELECT * FROM users WHERE passport = ?', (passport,))
    user = cur.fetchone()

    if not user:
        cur.close()
        conn.close()
        return jsonify({'error': 'Пользователь не найден'}), 404

    if USE_POSTGRESQL:
        cur.execute('''
            SELECT t.*, u1.full_name as from_name, u2.full_name as to_name
            FROM transactions t
            LEFT JOIN users u1 ON t.from_account = u1.account_number
            LEFT JOIN users u2 ON t.to_account = u2.account_number
            WHERE t.from_account = %s OR t.to_account = %s
            ORDER BY t.date DESC
            LIMIT 50
        ''', (user['account_number'], user['account_number']))
    else:
        cur.execute('''
            SELECT t.*, u1.full_name as from_name, u2.full_name as to_name
            FROM transactions t
            LEFT JOIN users u1 ON t.from_account = u1.account_number
            LEFT JOIN users u2 ON t.to_account = u2.account_number
            WHERE t.from_account = ? OR t.to_account = ?
            ORDER BY t.date DESC
            LIMIT 50
        ''', (user['account_number'], user['account_number']))
    transactions = cur.fetchall()

    user_dict = dict(user)
    transactions_list = []
    for t in transactions:
        transactions_list.append({
            'id': t['id'],
            'date': t['date'],
            'type': t['type'],
            'from_account': t['from_account'],
            'to_account': t['to_account'],
            'amount': t['amount'],
            'status': t['status'],
            'description': t['description'],
            'from_name': t['from_name'],
            'to_name': t['to_name']
        })

    cur.close()
    conn.close()

    return jsonify({
        'user': user_dict,
        'transactions': transactions_list
    })

@app.route('/admin/api/bulk_operations', methods=['POST'])
@require_permission('manage_users')
def admin_bulk_operations():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'Нет данных'}), 400
        action = data.get('action')
        passports = data.get('passports', [])

        if not action or not passports:
            return jsonify({'success': False, 'error': 'Неверные параметры'}), 400

        conn = get_db_connection()
        cur = conn.cursor()

        try:
            if action == 'block':
                placeholders = ','.join(['%s'] * len(passports)) if USE_POSTGRESQL else ','.join(['?'] * len(passports))
                if USE_POSTGRESQL:
                    cur.execute(f'UPDATE users SET is_active = FALSE WHERE passport IN ({placeholders})', passports)
                else:
                    cur.execute(f'UPDATE users SET is_active = 0 WHERE passport IN ({placeholders})', passports)
                flash_message = f'Заблокировано {len(passports)} пользователей'
            elif action == 'unblock':
                placeholders = ','.join(['%s'] * len(passports)) if USE_POSTGRESQL else ','.join(['?'] * len(passports))
                if USE_POSTGRESQL:
                    cur.execute(f'UPDATE users SET is_active = TRUE WHERE passport IN ({placeholders})', passports)
                else:
                    cur.execute(f'UPDATE users SET is_active = 1 WHERE passport IN ({placeholders})', passports)
                flash_message = f'Разблокировано {len(passports)} пользователей'
            elif action == 'reset_passwords':
                for passport in passports:
                    new_password = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
                    if USE_POSTGRESQL:
                        cur.execute('UPDATE users SET password_hash = %s WHERE passport = %s',
                                    (generate_password_hash(new_password), passport))
                    else:
                        cur.execute('UPDATE users SET password_hash = ? WHERE passport = ?',
                                    (generate_password_hash(new_password), passport))
                flash_message = f'Пароли сброшены для {len(passports)} пользователей'
            else:
                cur.close()
                conn.close()
                return jsonify({'success': False, 'error': 'Неизвестное действие'}), 400

            conn.commit()

            # Логирование
            if USE_POSTGRESQL:
                cur.execute('''
                    INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (session.get('passport'),
                      session.get('user_info', {}).get('full_name', 'Администратор'),
                      f'Групповое действие: {action}',
                      f'{len(passports)} пользователей',
                      f'Паспорта: {", ".join(passports[:5])}...'))
            else:
                cur.execute('''
                    INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                    VALUES (?, ?, ?, ?, ?)
                ''', (session.get('passport'),
                      session.get('user_info', {}).get('full_name', 'Администратор'),
                      f'Групповое действие: {action}',
                      f'{len(passports)} пользователей',
                      f'Паспорта: {", ".join(passports[:5])}...'))
            conn.commit()

            cur.close()
            conn.close()
            return jsonify({'success': True, 'message': flash_message})

        except Exception as e:
            conn.rollback()
            cur.close()
            conn.close()
            return jsonify({'success': False, 'error': str(e)}), 500

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ==================== РОЛИ ====================

@app.route('/admin/roles')
@require_permission('all_permissions')
def admin_roles():
    roles = get_all_roles()
    return render_template('admin_roles.html', roles=roles)

@app.route('/admin/roles/edit/<int:role_id>', methods=['GET', 'POST'])
@require_permission('all_permissions')
def edit_role(role_id):
    conn = get_db_connection()
    cur = conn.cursor()
    if request.method == 'POST':
        role_name = request.form['role_name']
        level = int(request.form['level'])
        description = request.form['description']
        if USE_POSTGRESQL:
            cur.execute('UPDATE roles SET role_name = %s, level = %s, description = %s WHERE id = %s',
                        (role_name, level, description, role_id))
        else:
            cur.execute('UPDATE roles SET role_name = ?, level = ?, description = ? WHERE id = ?',
                        (role_name, level, description, role_id))
        conn.commit()
        cur.close()
        conn.close()
        flash('Роль обновлена', 'success')
        return redirect(url_for('admin_roles'))

    if USE_POSTGRESQL:
        cur.execute('SELECT * FROM roles WHERE id = %s', (role_id,))
    else:
        cur.execute('SELECT * FROM roles WHERE id = ?', (role_id,))
    role = cur.fetchone()
    cur.close()
    conn.close()
    return render_template('edit_role.html', role=dict(role))

# ==================== ЗАПУСК ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
