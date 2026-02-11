import os
import sqlite3
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
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
app.secret_key = 'your-secret-key-here-change-this-in-production'
app.config['DATABASE'] = 'bank_system.db'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)
port = int(os.environ.get("PORT", 5000))
# ==================== ФУНКЦИИ БАЗЫ ДАННЫХ ====================

def get_db_connection():
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn

def row_to_dict(row):
    """Преобразует sqlite3.Row в словарь"""
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}

def init_db():
    conn = get_db_connection()
    
    # Таблица пользователей
    conn.execute('''
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
    
    # Таблица ролей
    conn.execute('''
        CREATE TABLE IF NOT EXISTS roles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            role_name TEXT UNIQUE NOT NULL,
            level INTEGER NOT NULL,
            permissions TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Таблица транзакций
    conn.execute('''
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
    
    # Таблица аудита
    conn.execute('''
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
    
    # Таблица бизнесов
    conn.execute('''
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
            status TEXT DEFAULT 'pending', -- pending, approved, rejected
            admin_notes TEXT,
            approved_by INTEGER,
            approved_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (approved_by) REFERENCES users (id)
        )
    ''')
    
    # Таблица бизнес-счетов
    conn.execute('''
        CREATE TABLE IF NOT EXISTS business_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id INTEGER NOT NULL,
            account_number TEXT UNIQUE NOT NULL,
            account_type TEXT DEFAULT 'current', -- current, savings, etc.
            balance REAL DEFAULT 0,
            currency TEXT DEFAULT 'RUB',
            is_active BOOLEAN DEFAULT 1,
            credit_limit REAL DEFAULT 0,
            overdraft_allowed BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (business_id) REFERENCES businesses (id)
        )
    ''')
    
    # Таблица заявок на вывод средств
    conn.execute('''
        CREATE TABLE IF NOT EXISTS withdrawal_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_account_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            purpose TEXT NOT NULL,
            recipient_name TEXT,
            recipient_account TEXT,
            recipient_bank TEXT,
            status TEXT DEFAULT 'pending', -- pending, approved, rejected, processing
            admin_notes TEXT,
            processed_by INTEGER,
            processed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (business_account_id) REFERENCES business_accounts (id),
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (processed_by) REFERENCES users (id)
        )
    ''')
    
    # Таблица NFC-меток (исправленная - привязка к обычным пользователям)
    conn.execute('DROP TABLE IF EXISTS nfc_tags')
    conn.execute('''
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
    
    # Таблица PIN-кодов
    conn.execute('DROP TABLE IF EXISTS user_pins')
    conn.execute('''
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
    
    # Таблица сессий оплаты
    conn.execute('DROP TABLE IF EXISTS payment_sessions')
    conn.execute('''
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
    
    # Стандартные роли (добавляем бизнес-роль)
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
        conn.execute('''
            INSERT OR IGNORE INTO roles (id, role_name, level, permissions, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (role_id, role_name, level, permissions, description))
    
    # Создаем суперадмина
    admin = conn.execute('SELECT * FROM users WHERE passport = ?', ('admin001',)).fetchone()
    if not admin:
        conn.execute('''
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
    
    # Тестовые пользователи
    test_users = [
        ('special001', 'Специальный Админ', 'SPEC001', 50000, 2, 'special123'),
        ('admin002', 'Обычный Админ', 'ADMIN002', 30000, 3, 'admin123'),
        ('invest001', 'Цифровой Следователь', 'INVEST001', 20000, 4, 'invest123'),
        ('regist001', 'Регистратор Паспортов', 'REGIST001', 15000, 5, 'regist123'),
        ('user002', 'Обычный Пользователь', 'USER002', 5000, 6, 'user123'),
        ('912312', 'КИЯМОВ КАРИМ МАРАТОВИЧ', f'ACC{random.randint(100000, 999999)}', 1000, 6, '123456')
    ]
    
    for passport, full_name, account_number, balance, role_id, password in test_users:
        existing = conn.execute('SELECT * FROM users WHERE passport = ?', (passport,)).fetchone()
        if not existing:
            conn.execute('''
                INSERT INTO users (passport, full_name, account_number, balance, role_id, password_hash)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (passport, full_name, account_number, balance, role_id, generate_password_hash(password)))
            print(f"✅ Создан тестовый пользователь: {passport} / {password}")
    
    # Создаем тестовый бизнес для демонстрации
    test_business_user = conn.execute('SELECT * FROM users WHERE passport = ?', ('user002',)).fetchone()
    if test_business_user:
        # Создаем тестовый бизнес
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO businesses (user_id, business_name, charter_capital, status, email, phone)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            test_business_user['id'],
            'Тестовый Бизнес ООО',
            50000,
            'approved',
            'test_business@example.com',
            '+79990000001'
        ))
        
        business_id = cursor.lastrowid
        
        # Создаем бизнес-счет
        cursor.execute('''
            INSERT OR IGNORE INTO business_accounts (business_id, account_number, balance)
            VALUES (?, ?, ?)
        ''', (
            business_id,
            f'BUS{random.randint(100000, 999999)}',
            50000
        ))
        
        print("✅ Создан тестовый бизнес с балансом 50,000 ₽")
    
    # Удаляем старые индексы, если они существуют
    try:
        conn.execute('DROP INDEX IF EXISTS idx_nfc_tags_business')
        conn.execute('DROP INDEX IF EXISTS idx_nfc_tags_user')
    except:
        pass
    
    # Создаем индексы
    conn.execute('CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions (from_account, to_account)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_nfc_tags_user ON nfc_tags (user_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_payment_sessions_session ON payment_sessions (session_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_payment_sessions_status ON payment_sessions (status)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_payment_sessions_buyer ON payment_sessions (buyer_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_payment_sessions_seller ON payment_sessions (seller_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_businesses_user ON businesses (user_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_businesses_status ON businesses (status)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_business_accounts_business ON business_accounts (business_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_withdrawal_requests_account ON withdrawal_requests (business_account_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_withdrawal_requests_status ON withdrawal_requests (status)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_user_pins_user_tag ON user_pins (user_id, nfc_tag_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log (timestamp)')
    
    conn.commit()
    conn.close()
    print("✅ База данных инициализирована")

def find_user_by_passport(passport):
    conn = get_db_connection()
    user = conn.execute('''
        SELECT u.*, r.role_name, r.level, r.permissions 
        FROM users u
        LEFT JOIN roles r ON u.role_id = r.id
        WHERE u.passport = ?
    ''', (passport,)).fetchone()
    conn.close()
    return dict(user) if user else None

def find_user_by_account(account_number):
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE account_number = ?', (account_number,)).fetchone()
    conn.close()
    return dict(user) if user else None

def find_user_by_id(user_id):
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
    conn.close()
    return dict(user) if user else None

def get_user_role(user_id):
    conn = get_db_connection()
    role = conn.execute('''
        SELECT r.* FROM roles r
        JOIN users u ON r.id = u.role_id
        WHERE u.id = ?
    ''', (user_id,)).fetchone()
    conn.close()
    return dict(role) if role else None

def get_role_by_name(role_name):
    conn = get_db_connection()
    role = conn.execute('SELECT * FROM roles WHERE role_name = ?', (role_name,)).fetchone()
    conn.close()
    return dict(role) if role else None

def get_role_by_id(role_id):
    conn = get_db_connection()
    role = conn.execute('SELECT * FROM roles WHERE id = ?', (role_id,)).fetchone()
    conn.close()
    return dict(role) if role else None

def get_all_roles():
    conn = get_db_connection()
    roles = conn.execute('SELECT * FROM roles ORDER BY level DESC').fetchall()
    conn.close()
    return [dict(role) for role in roles]

def get_all_users():
    conn = get_db_connection()
    users = conn.execute('''
        SELECT u.*, r.role_name,
               CASE WHEN u.role_id <= 3 THEN 1 ELSE 0 END as is_admin
        FROM users u
        LEFT JOIN roles r ON u.role_id = r.id
        ORDER BY u.created_at DESC
    ''').fetchall()
    conn.close()
    return [dict(user) for user in users]

def update_user_balance(account_number, new_balance):
    conn = get_db_connection()
    conn.execute('UPDATE users SET balance = ? WHERE account_number = ?', (new_balance, account_number))
    conn.commit()
    conn.close()

def add_transaction(transaction_type, from_account, to_account, amount, status, description, user_id=None):
    conn = get_db_connection()
    conn.execute('''
        INSERT INTO transactions (type, from_account, to_account, amount, status, description, user_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (transaction_type, from_account, to_account, amount, status, description, user_id))
    conn.commit()
    conn.close()

def get_user_transactions(account_number, limit=10):
    conn = get_db_connection()
    transactions = conn.execute('''
        SELECT * FROM transactions
        WHERE from_account = ? OR to_account = ?
        ORDER BY date DESC
        LIMIT ?
    ''', (account_number, account_number, limit)).fetchall()
    conn.close()
    return [dict(transaction) for transaction in transactions]

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

def send_email(to_email, subject, body, html_body=None):
    """Отправка email через SMTP"""
    try:
        smtp_server = "smtp.gmail.com"
        smtp_port = 587
        smtp_username = "dvorpay@gmail.com"
        smtp_password = "987dvorpay987"
        
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"DvorPay <{smtp_username}>"
        msg['To'] = to_email
        
        text_part = MIMEText(body, 'plain', 'utf-8')
        msg.attach(text_part)
        
        if html_body:
            html_part = MIMEText(html_body, 'html', 'utf-8')
            msg.attach(html_part)
        
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
        
        print(f"✅ Email отправлен на {to_email}")
        return True
    except Exception as e:
        print(f"❌ Ошибка отправки email: {e}")
        return False

def send_business_approval_email(to_email, business_name, account_number, password, capital):
    """Отправка email об одобрении бизнеса"""
    subject = f"Ваша заявка на бизнес '{business_name}' одобрена"
    
    body = f"""
    Уважаемый владелец бизнеса,
    
    Ваша заявка на создание бизнеса "{business_name}" была одобрена.
    
    Ваши данные для входа в бизнес-кабинет:
    • Логин (паспорт): BUS{account_number}
    • Пароль: {password}
    
    Информация о бизнес-счете:
    • Номер счета: {account_number}
    • Уставной капитал: {capital} ₽
    • Валюта: RUB
    
    Войдите в систему по ссылке: http://ваш-сайт/dashboard
    
    Рекомендуем изменить пароль после первого входа.
    
    С уважением,
    Команда DvorPay
    """
    
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 10px;">
            <h2 style="color: #2c5aa0;">🎉 Заявка на бизнес одобрена!</h2>
            
            <p>Ваша заявка на создание бизнеса <strong>{business_name}</strong> была одобрена.</p>
            
            <div style="background: #f5f5f5; padding: 15px; border-radius: 5px; margin: 20px 0;">
                <h3 style="color: #2c5aa0;">📋 Данные для входа:</h3>
                <p><strong>Логин (паспорт):</strong> BUS{account_number}</p>
                <p><strong>Пароль:</strong> {password}</p>
            </div>
            
            <div style="background: #f0f8ff; padding: 15px; border-radius: 5px; margin: 20px 0;">
                <h3 style="color: #2c5aa0;">💳 Информация о счете:</h3>
                <p><strong>Номер счета:</strong> {account_number}</p>
                <p><strong>Уставной капитал:</strong> {capital} ₽</p>
                <p><strong>Валюта:</strong> RUB</p>
            </div>
            
            <div style="text-align: center; margin: 30px 0;">
                <a href="http://ваш-сайт/dashboard" style="background: #2c5aa0; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; display: inline-block;">
                    Войти в систему
                </a>
            </div>
            
            <p style="color: #666; font-size: 0.9em; margin-top: 30px;">
                Рекомендуем изменить пароль после первого входа.<br>
                Это письмо отправлено автоматически, пожалуйста, не отвечайте на него.
            </p>
            
            <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
            
            <p style="color: #999; font-size: 0.8em;">
                С уважением,<br>
                Команда DvorPay
            </p>
        </div>
    </body>
    </html>
    """
    
    return send_email(to_email, subject, body, html_body)

def send_business_rejection_email(to_email, business_name, reason):
    """Отправка email об отклонении бизнеса"""
    subject = f"Заявка на бизнес '{business_name}' отклонена"
    
    body = f"""
    Уважаемый заявитель,
    
    К сожалению, ваша заявка на создание бизнеса "{business_name}" была отклонена.
    
    Причина: {reason}
    
    Вы можете исправить указанные проблемы и подать заявку повторно.
    
    С уважением,
    Команда DvorPay
    """
    
    return send_email(to_email, subject, body)

def send_withdrawal_notification_email(to_email, amount, status, notes=None):
    """Отправка уведомления о статусе вывода средств"""
    status_text = "одобрена" if status == 'approved' else "отклонена"
    
    subject = f"Заявка на вывод средств {status_text}"
    
    body = f"""
    Уважаемый клиент,
    
    Ваша заявка на вывод средств в размере {amount} ₽ была {status_text}.
    
    {f"Комментарий администратора: {notes}" if notes else ""}
    
    С уважением,
    Команда DvorPay
    """
    
    return send_email(to_email, subject, body)

def create_business_application(user_id, business_name, charter_capital, legal_name=None, tax_id=None, 
                                address=None, email=None, phone=None):
    """Создать заявку на создание бизнеса"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO businesses (user_id, business_name, charter_capital, legal_name, tax_id, 
                                   address, email, phone, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
        ''', (user_id, business_name, charter_capital, legal_name, tax_id, address, email, phone))
        
        application_id = cursor.lastrowid
        conn.commit()
        
        user = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
        conn.execute('''
            INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
            VALUES (?, ?, ?, ?, ?)
        ''', ('SYSTEM', 'Система', 'Подача заявки на бизнес', 
              user['passport'] if user else str(user_id),
              f'Название: {business_name}, Уставной капитал: {charter_capital}'))
        conn.commit()
        
        return application_id
    except sqlite3.IntegrityError as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_business_applications(status=None):
    """Получить список заявок на бизнес"""
    conn = get_db_connection()
    
    query = '''
        SELECT b.*, u.passport, u.full_name, u.email as user_email,
               a.full_name as approved_by_name
        FROM businesses b
        JOIN users u ON b.user_id = u.id
        LEFT JOIN users a ON b.approved_by = a.id
    '''
    
    params = []
    if status:
        query += ' WHERE b.status = ?'
        params.append(status)
    
    query += ' ORDER BY b.created_at DESC'
    
    applications = conn.execute(query, params).fetchall()
    conn.close()
    
    return [dict(app) for app in applications]

def get_business_by_id(business_id):
    """Получить бизнес по ID"""
    conn = get_db_connection()
    business = conn.execute('''
        SELECT b.*, u.passport, u.full_name, u.email as user_email
        FROM businesses b
        JOIN users u ON b.user_id = u.id
        WHERE b.id = ?
    ''', (business_id,)).fetchone()
    conn.close()
    return dict(business) if business else None

def approve_business_application(business_id, admin_id, admin_notes=None):
    """Одобрить заявку на бизнес"""
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE businesses 
            SET status = 'approved', 
                approved_by = ?,
                approved_at = CURRENT_TIMESTAMP,
                admin_notes = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (admin_id, admin_notes, business_id))
        
        business = conn.execute('SELECT * FROM businesses WHERE id = ?', (business_id,)).fetchone()
        business = row_to_dict(business)
        
        account_number = f'BUS{random.randint(100000, 999999)}'
        cursor.execute('''
            INSERT INTO business_accounts (business_id, account_number, balance)
            VALUES (?, ?, ?)
        ''', (business_id, account_number, business['charter_capital']))
        
        user = conn.execute('SELECT * FROM users WHERE id = ?', (business['user_id'],)).fetchone()
        user = row_to_dict(user)
        
        business_password = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        
        cursor.execute('''
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
        
        business_user_id = cursor.lastrowid
        
        conn.execute('''
            INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
            VALUES (?, ?, ?, ?, ?)
        ''', ('SYSTEM', 'Система', 'Создание бизнес-аккаунта', 
              user['passport'],
              f'Бизнес: {business["business_name"]}, Счет: {account_number}'))
        
        conn.commit()
        
        email_to = business.get('email') or user.get('email')
        if email_to:
            send_business_approval_email(
                email_to,
                business['business_name'],
                account_number,
                business_password,
                business['charter_capital']
            )
        
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
        conn.close()

def reject_business_application(business_id, admin_id, admin_notes):
    """Отклонить заявку на бизнес"""
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE businesses 
            SET status = 'rejected', 
                approved_by = ?,
                admin_notes = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (admin_id, admin_notes, business_id))
        
        business = conn.execute('SELECT * FROM businesses WHERE id = ?', (business_id,)).fetchone()
        business = row_to_dict(business)
        
        if business:
            user = conn.execute('SELECT * FROM users WHERE id = ?', (business['user_id'],)).fetchone()
            user = row_to_dict(user)
            
            conn.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (?, ?, ?, ?, ?)
            ''', ('SYSTEM', 'Система', 'Отклонение заявки на бизнес', 
                  user['passport'] if user else str(business['user_id']),
                  f'Причина: {admin_notes}'))
            
            conn.commit()
            
            email_to = business.get('email') or (user.get('email') if user else None)
            if email_to:
                send_business_rejection_email(
                    email_to,
                    business['business_name'],
                    admin_notes
                )
        
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def create_withdrawal_request(business_account_id, user_id, amount, purpose, 
                              recipient_name=None, recipient_account=None, recipient_bank=None):
    """Создать заявку на вывод средств"""
    conn = get_db_connection()
    
    try:
        account = conn.execute('SELECT * FROM business_accounts WHERE id = ?', (business_account_id,)).fetchone()
        if account['balance'] < amount:
            raise ValueError("Недостаточно средств на счете")
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO withdrawal_requests 
            (business_account_id, user_id, amount, purpose, recipient_name, recipient_account, recipient_bank, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
        ''', (business_account_id, user_id, amount, purpose, recipient_name, recipient_account, recipient_bank))
        
        request_id = cursor.lastrowid
        
        conn.execute('''
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
        conn.close()

def get_withdrawal_requests(status=None):
    """Получить список заявок на вывод"""
    conn = get_db_connection()
    
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
        query += ' WHERE wr.status = ?'
        params.append(status)
    
    query += ' ORDER BY wr.created_at DESC'
    
    requests = conn.execute(query, params).fetchall()
    conn.close()
    
    return [dict(req) for req in requests]

def process_withdrawal_request(request_id, admin_id, status, admin_notes=None):
    """Обработать заявку на вывод средств"""
    conn = get_db_connection()
    
    try:
        request = conn.execute('''
            SELECT wr.*, ba.balance, ba.account_number
            FROM withdrawal_requests wr
            JOIN business_accounts ba ON wr.business_account_id = ba.id
            WHERE wr.id = ?
        ''', (request_id,)).fetchone()
        
        if not request:
            raise ValueError("Заявка не найдена")
        
        if request['status'] != 'pending':
            raise ValueError("Заявка уже обработана")
        
        if status == 'approved':
            if request['balance'] < request['amount']:
                raise ValueError("Недостаточно средств на счете")
            
            new_balance = request['balance'] - request['amount']
            conn.execute('''
                UPDATE business_accounts 
                SET balance = ?
                WHERE id = ?
            ''', (new_balance, request['business_account_id']))
            
            add_transaction(
                'Вывод с бизнес-счета',
                request['account_number'],
                'Банк',
                request['amount'],
                'Успешно',
                f'Вывод средств: {request["purpose"]}'
            )
        
        conn.execute('''
            UPDATE withdrawal_requests 
            SET status = ?,
                processed_by = ?,
                processed_at = CURRENT_TIMESTAMP,
                admin_notes = ?
            WHERE id = ?
        ''', (status, admin_id, admin_notes, request_id))
        
        conn.execute('''
            INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
            VALUES (?, ?, ?, ?, ?)
        ''', ('SYSTEM', 'Система', f'Обработка заявки на вывод: {status}', 
              str(request['user_id']),
              f'Сумма: {request["amount"]}, Статус: {status}'))
        
        conn.commit()
        
        user = conn.execute('SELECT * FROM users WHERE id = ?', (request['user_id'],)).fetchone()
        if user and user['email']:
            send_withdrawal_notification_email(
                user['email'],
                request['amount'],
                status,
                admin_notes
            )
        
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# ==================== NFC ФУНКЦИИ ====================

def create_pin_for_nfc(user_id, nfc_tag_id, pin):
    """Создать PIN-код для NFC-метки"""
    conn = get_db_connection()
    
    salt = secrets.token_hex(16)
    pin_hash = hashlib.sha256((pin + salt).encode()).hexdigest()
    
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO user_pins (user_id, nfc_tag_id, pin_hash, pin_salt)
        VALUES (?, ?, ?, ?)
    ''', (user_id, nfc_tag_id, pin_hash, salt))
    
    conn.commit()
    conn.close()
    return pin

def verify_pin(user_id, nfc_tag_id, pin):
    """Проверить PIN-код для NFC-метки"""
    conn = get_db_connection()
    pin_data = conn.execute('''
        SELECT * FROM user_pins 
        WHERE user_id = ? AND nfc_tag_id = ? AND is_locked = 0
    ''', (user_id, nfc_tag_id)).fetchone()
    
    if not pin_data:
        conn.close()
        return False
    
    if pin_data['attempts'] >= 5:
        conn.execute('UPDATE user_pins SET is_locked = 1 WHERE id = ?', (pin_data['id'],))
        conn.commit()
        conn.close()
        return False
    
    salt = pin_data['pin_salt']
    pin_hash = hashlib.sha256((pin + salt).encode()).hexdigest()
    
    if pin_hash == pin_data['pin_hash']:
        conn.execute('UPDATE user_pins SET attempts = 0 WHERE id = ?', (pin_data['id'],))
        conn.commit()
        conn.close()
        return True
    else:
        conn.execute('''
            UPDATE user_pins 
            SET attempts = attempts + 1, last_attempt = CURRENT_TIMESTAMP 
            WHERE id = ?
        ''', (pin_data['id'],))
        conn.commit()
        conn.close()
        return False

def generate_nfc_url(nfc_tag_id):
    """Сгенерировать уникальную ссылку для NFC"""
    unique_token = secrets.token_urlsafe(32)
    return f"/nfc/pay/{nfc_tag_id}/{unique_token}"

# ==================== ДЕКОРАТОРЫ ====================

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

# ==================== МАРШРУТЫ ====================

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
    
    return render_template('dashboard.html', 
                         user=user_info, 
                         transactions=transactions)

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
        conn.execute('UPDATE users SET password_hash = ? WHERE passport = ?', 
                    (generate_password_hash(new_password), session.get('passport')))
        conn.commit()
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
        return jsonify({
            'name': user['full_name'],
            'account': user['account_number']
        })
    else:
        return jsonify({'error': 'Пользователь не найден'})

# ==================== БИЗНЕС МАРШРУТЫ ====================

@app.route('/business/apply', methods=['GET', 'POST'])
def business_apply():
    """Подача заявки на создание бизнеса"""
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
                session['user_id'],
                business_name,
                charter_capital,
                legal_name,
                tax_id,
                address,
                email,
                phone
            )
            
            flash('Заявка успешно отправлена! Ожидайте решения администратора.', 'success')
            return redirect(url_for('dashboard'))
            
        except Exception as e:
            flash(f'Ошибка при подаче заявки: {str(e)}', 'error')
    
    return render_template('business_apply.html')

@app.route('/admin/business_applications')
@require_permission('manage_users')
def admin_business_applications():
    """Страница заявок на бизнес"""
    status = request.args.get('status', 'pending')
    applications = get_business_applications(status)
    return render_template('admin_business_applications.html', 
                         applications=applications, 
                         status=status)

@app.route('/admin/business_applications/view/<int:business_id>')
@require_permission('manage_users')
def view_business_application(business_id):
    """Просмотр заявки на бизнес"""
    business = get_business_by_id(business_id)
    if not business:
        flash('Заявка не найдена', 'error')
        return redirect(url_for('admin_business_applications'))
    
    return render_template('business_application_view.html', business=business)

@app.route('/admin/business_applications/approve/<int:business_id>', methods=['POST'])
@require_permission('manage_users')
def approve_business_application_route(business_id):
    """Одобрить заявку на бизнес"""
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
    """Отклонить заявку на бизнес"""
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
    """Заявка на вывод средств с бизнес-счета"""
    if not session.get('logged_in'):
        return redirect(url_for('index'))
    
    conn = get_db_connection()
    
    business_accounts = conn.execute('''
        SELECT ba.*, b.business_name
        FROM business_accounts ba
        JOIN businesses b ON ba.business_id = b.id
        WHERE b.user_id = ? AND b.status = 'approved' AND ba.is_active = 1
    ''', (session['user_id'],)).fetchall()
    
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
                return render_template('business_withdraw.html', accounts=business_accounts)
            
            request_id = create_withdrawal_request(
                business_account_id,
                session['user_id'],
                amount,
                purpose,
                recipient_name,
                recipient_account,
                recipient_bank
            )
            
            flash('Заявка на вывод средств отправлена на рассмотрение', 'success')
            return redirect(url_for('dashboard'))
            
        except ValueError as e:
            flash(str(e), 'error')
        except Exception as e:
            flash(f'Ошибка: {str(e)}', 'error')
    
    conn.close()
    return render_template('business_withdraw.html', 
                         accounts=[dict(acc) for acc in business_accounts])

@app.route('/admin/withdrawal_requests')
@require_permission('manage_users')
def admin_withdrawal_requests():
    """Страница заявок на вывод средств"""
    status = request.args.get('status', 'pending')
    requests = get_withdrawal_requests(status)
    return render_template('admin_withdrawal_requests.html', 
                         requests=requests, 
                         status=status)

@app.route('/admin/withdrawal_requests/view/<int:request_id>')
@require_permission('manage_users')
def view_withdrawal_request(request_id):
    """Просмотр заявки на вывод средств"""
    conn = get_db_connection()
    
    request_data = conn.execute('''
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
    ''', (request_id,)).fetchone()
    
    conn.close()
    
    if not request_data:
        flash('Заявка не найдена', 'error')
        return redirect(url_for('admin_withdrawal_requests'))
    
    return render_template('withdrawal_request_view.html', 
                         request=dict(request_data))

@app.route('/admin/withdrawal_requests/process/<int:request_id>', methods=['POST'])
@require_permission('manage_users')
def process_withdrawal_request_route(request_id):
    """Обработать заявку на вывод средств"""
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
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'register':
            user_id = request.form.get('user_id')
            tag_uid = request.form.get('tag_uid', '').upper().strip()
            pin_code = request.form.get('pin_code', '0000')
            
            # ДЕБАГ: Проверяем, что приходит из формы
            print(f"🔍 DEBUG: user_id из формы: {user_id}")
            print(f"🔍 DEBUG: tag_uid из формы: {tag_uid}")
            print(f"🔍 DEBUG: action из формы: {action}")
            if not user_id:
                # Если вдруг передали паспорт – найдём ID
                passport = request.form.get('passport')
                if passport:
                    user = find_user_by_passport(passport)
                    if user:
                        user_id = user['id']
            if not user_id:
                flash('Не выбран пользователь', 'error')
                return redirect(url_for('admin_nfc'))
            
            if not tag_uid:
                flash('Введите UID NFC-метки', 'error')
                return redirect(url_for('admin_nfc'))
            
            # Проверяем права доступа
            if session.get('role') not in ['super_admin', 'special_admin', 'admin']:
                flash('У вас нет прав для регистрации NFC-меток', 'error')
                return redirect(url_for('admin_nfc'))
            
            existing = conn.execute('SELECT * FROM nfc_tags WHERE tag_uid = ?', (tag_uid,)).fetchone()
            if existing:
                flash('NFC-метка с таким UID уже зарегистрирована', 'error')
                return redirect(url_for('admin_nfc'))
            
            # Создаем NFC-метку
            cursor = conn.cursor()
            tag_url = f"/nfc/pay/0/temp"
            cursor.execute('''
                INSERT INTO nfc_tags (user_id, tag_uid, tag_url) 
                VALUES (?, ?, ?)
            ''', (user_id, tag_uid, tag_url))
            nfc_tag_id = cursor.lastrowid
            
            # Обновляем ссылку
            real_url = generate_nfc_url(nfc_tag_id)
            conn.execute('UPDATE nfc_tags SET tag_url = ? WHERE id = ?', (real_url, nfc_tag_id))
            
            # Создаем PIN-код
            create_pin_for_nfc(user_id, nfc_tag_id, pin_code)
            
            conn.commit()
            
            # Получаем имя пользователя для сообщения
            user = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
            if user:
                flash(f'NFC-метка зарегистрирована для пользователя {user["full_name"]}. PIN: {pin_code}', 'success')
            else:
                flash(f'NFC-метка зарегистрирована. PIN: {pin_code}', 'success')
    
    # Получаем всех пользователей для выпадающего списка
    users = conn.execute('''
        SELECT u.id, u.full_name, u.passport, u.account_number, r.role_name
        FROM users u
        JOIN roles r ON u.role_id = r.id
        WHERE u.is_active = 1 AND r.role_name IN ('user', 'business')
        ORDER BY u.full_name
    ''').fetchall()
    
    # Получаем NFC-метки
    nfc_tags = conn.execute('''
        SELECT n.*, u.full_name, u.account_number, r.role_name,
               up.attempts, up.is_locked, up.last_attempt
        FROM nfc_tags n
        JOIN users u ON n.user_id = u.id
        JOIN roles r ON u.role_id = r.id
        LEFT JOIN user_pins up ON n.id = up.nfc_tag_id AND u.id = up.user_id
        ORDER BY n.created_at DESC
    ''').fetchall()
    
    conn.close()
    
    print(f"🔍 DEBUG: Количество пользователей для формы: {len(users)}")
    for user in users:
        print(f"  - {user['id']}: {user['full_name']} ({user['role_name']})")
    
    return render_template('admin_nfc.html', 
                         nfc_tags=[dict(t) for t in nfc_tags], 
                         users=[dict(u) for u in users])

@app.route('/nfc/pay/<int:nfc_tag_id>/<token>')
def nfc_payment_page(nfc_tag_id, token):
    """Страница оплаты по NFC (только для бизнес-счетов)"""
    if not session.get('logged_in'):
        flash('Пожалуйста, войдите в систему', 'error')
        return redirect(url_for('index'))
    
    conn = get_db_connection()
    
    nfc_tag = conn.execute('''
        SELECT n.*, u.full_name, u.account_number, u.balance, u.email
        FROM nfc_tags n
        JOIN users u ON n.user_id = u.id
        WHERE n.id = ? AND n.is_active = 1
    ''', (nfc_tag_id,)).fetchone()
    
    if not nfc_tag:
        conn.close()
        return render_template('nfc_error.html', error="NFC-метка не найдена или заблокирована")
    
    seller = conn.execute('''
        SELECT u.*, r.role_name 
        FROM users u
        JOIN roles r ON u.role_id = r.id
        WHERE u.id = ?
    ''', (session['user_id'],)).fetchone()
    
    if seller['role_name'] != 'business':
        conn.close()
        return render_template('nfc_error.html', 
                             error="Оплату могут принимать только бизнес-счета")
    
    if nfc_tag['user_id'] == seller['id']:
        conn.close()
        return render_template('nfc_error.html', 
                             error="Вы не можете оплачивать сами себе")
    
    session_id = secrets.token_urlsafe(32)
    
    conn.execute('''
        INSERT INTO payment_sessions (session_id, buyer_id, seller_id, expires_at)
        VALUES (?, ?, ?, ?)
    ''', (session_id, nfc_tag['user_id'], seller['id'], 
          datetime.now() + timedelta(minutes=10)))
    
    conn.commit()
    
    nfc_tag_dict = dict(nfc_tag)
    seller_dict = dict(seller)
    
    conn.close()
    
    return render_template('nfc_payment.html',
                         buyer={
                             'full_name': nfc_tag_dict['full_name'],
                             'account_number': nfc_tag_dict['account_number'],
                             'balance': nfc_tag_dict['balance']
                         },
                         seller={
                             'full_name': seller_dict['full_name'],
                             'account_number': seller_dict['account_number'],
                             'business_name': seller_dict.get('full_name', 'Бизнес')
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
    """Страница просмотра всех транзакций"""
    conn = get_db_connection()
    
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
        query += ' AND DATE(t.date) >= ?'
        params.append(date_from)
    
    if date_to:
        query += ' AND DATE(t.date) <= ?'
        params.append(date_to)
    
    if account:
        query += ' AND (t.from_account LIKE ? OR t.to_account LIKE ?)'
        params.append(f'%{account}%')
        params.append(f'%{account}%')
    
    query += ' ORDER BY t.date DESC LIMIT 100'
    
    transactions = conn.execute(query, params).fetchall()
    conn.close()
    
    return render_template('admin_transactions.html', 
                         transactions=[dict(t) for t in transactions])

@app.route('/admin/audit_logs')
@require_permission('audit_logs')
def admin_audit_logs():
    """Страница логов аудита"""
    conn = get_db_connection()
    logs = conn.execute('''
        SELECT * FROM audit_log 
        ORDER BY timestamp DESC 
        LIMIT 100
    ''').fetchall()
    conn.close()
    
    return render_template('admin_audit.html', logs=[dict(log) for log in logs])

@app.route('/admin/system_settings')
@require_permission('all_permissions')
def admin_system_settings():
    """Страница настроек системы"""
    return render_template('admin_system_settings.html')

@app.route('/admin/backup')
@require_permission('all_permissions')
def admin_backup():
    """Страница резервного копирования"""
    return render_template('admin_backup.html')

@app.route('/admin/register_nfc')
@require_permission('manage_nfc')
def admin_register_nfc():
    """Перенаправление на страницу NFC (для обратной совместимости)"""
    return redirect(url_for('admin_nfc'))

@app.route('/admin/add_user', methods=['GET','POST'])
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
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO users (passport, full_name, account_number, balance, role_id, password_hash, email, phone)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (passport, full_name, account_number, balance, role_id, generate_password_hash(password), email, phone))
        user_id = cursor.lastrowid
        conn.commit()
        flash(f'Пользователь успешно добавлен (ID: {user_id})', 'success')
    except sqlite3.IntegrityError:
        flash('Пользователь с таким паспортом или номером счета уже существует', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('admin_users'))

@app.route('/admin/change_role/<passport>', methods=['GET', 'POST'])
@require_permission('manage_users')
def change_user_role(passport):
    """Изменение роли пользователя"""
    conn = get_db_connection()
    
    user = conn.execute('SELECT * FROM users WHERE passport = ?', (passport,)).fetchone()
    if not user:
        flash('Пользователь не найден', 'error')
        return redirect(url_for('admin_users'))
    
    roles = conn.execute('SELECT * FROM roles WHERE id > 1 ORDER BY level DESC').fetchall()
    
    if request.method == 'POST':
        new_role_id = int(request.form['role_id'])
        
        if user['role_id'] == 1 and session.get('role') != 'super_admin':
            flash('Нельзя изменять роль суперадмина', 'error')
            return redirect(url_for('admin_users'))
        
        conn.execute('UPDATE users SET role_id = ? WHERE passport = ?', (new_role_id, passport))
        
        new_role = conn.execute('SELECT * FROM roles WHERE id = ?', (new_role_id,)).fetchone()
        
        conn.execute('''
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
    
    current_role = conn.execute('SELECT * FROM roles WHERE id = ?', (user['role_id'],)).fetchone()
    conn.close()
    
    return render_template('change_role.html', 
                         user=dict(user), 
                         current_role=dict(current_role) if current_role else None,
                         roles=[dict(role) for role in roles])

@app.route('/admin/toggle_block/<passport>')
@require_permission('manage_users')
def toggle_block_user(passport):
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE passport = ?', (passport,)).fetchone()
    if user:
        new_status = 0 if user['is_active'] else 1
        conn.execute('UPDATE users SET is_active = ? WHERE passport = ?', (new_status, passport))
        conn.commit()
        
        conn.execute('''
            INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
            VALUES (?, ?, ?, ?, ?)
        ''', (session.get('passport'), 
              session.get('user_info', {}).get('full_name', 'Администратор'),
              'Изменение статуса блокировки',
              passport,
              f'Новый статус: {"разблокирован" if new_status else "заблокирован"}'))
        conn.commit()
        
        flash(f'Пользователь {passport} {"разблокирован" if new_status else "заблокирован"}', 'success')
    
    conn.close()
    return redirect(url_for('admin_users'))

@app.route('/admin/toggle_admin/<passport>')
@require_permission('manage_users')
def toggle_admin_status_route(passport):
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE passport = ?', (passport,)).fetchone()
    if user:
        new_role_id = 6 if user['role_id'] <= 3 else 3
        conn.execute('UPDATE users SET role_id = ? WHERE passport = ?', (new_role_id, passport))
        conn.commit()
        
        role_name = "администратором" if new_role_id <= 3 else "обычным пользователем"
        flash(f'Пользователь {passport} назначен {role_name}', 'success')
    
    conn.close()
    return redirect(url_for('admin_users'))

@app.route('/admin/reset_password/<passport>')
@require_permission('manage_users')
def reset_password(passport):
    new_password = ''.join(random.choices(string.digits, k=8))
    
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE passport = ?', (passport,)).fetchone()
    if user:
        conn.execute('UPDATE users SET password_hash = ? WHERE passport = ?', 
                    (generate_password_hash(new_password), passport))
        conn.commit()
        flash(f'Пароль для пользователя {passport} сброшен. Новый пароль: {new_password}', 'success')
    else:
        flash('Пользователь не найден', 'error')
    
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
    """Детальная информация о NFC-метке"""
    conn = get_db_connection()
    
    nfc_tag = conn.execute('''
        SELECT n.*, u.passport, u.full_name, u.account_number, 
               u.balance, u.email, u.phone, u.created_at as user_created
        FROM nfc_tags n
        JOIN users u ON n.user_id = u.id
        WHERE n.id = ?
    ''', (nfc_id,)).fetchone()
    
    if not nfc_tag:
        flash('NFC-метка не найдена', 'error')
        return redirect(url_for('admin_nfc'))
    
    transactions = conn.execute('''
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
        WHERE u.id = ? OR u.id = ?
        ORDER BY t.date DESC
        LIMIT 50
    ''', (nfc_tag['user_id'], nfc_tag['user_id'])).fetchall()
    
    stats = conn.execute('''
        SELECT 
            COUNT(*) as total_transactions,
            SUM(CASE WHEN t.from_account = u.account_number THEN t.amount ELSE 0 END) as total_sent,
            SUM(CASE WHEN t.to_account = u.account_number THEN t.amount ELSE 0 END) as total_received,
            MAX(t.date) as last_transaction
        FROM transactions t
        JOIN users u ON (t.from_account = u.account_number OR t.to_account = u.account_number)
        WHERE u.id = ?
    ''', (nfc_tag['user_id'],)).fetchone()
    
    pin_info = conn.execute('''
        SELECT attempts, is_locked, last_attempt, created_at
        FROM user_pins
        WHERE nfc_tag_id = ?
    ''', (nfc_id,)).fetchone()
    
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
    session = conn.execute('''
        SELECT ps.*, s.account_number as seller_account
        FROM payment_sessions ps
        JOIN users s ON ps.seller_id = s.id
        WHERE ps.session_id = ? AND ps.status = 'pending'
    ''', (session_id,)).fetchone()
    
    if not session:
        conn.close()
        return jsonify({'success': False, 'error': 'Сессия не найдена'})
    
    conn.execute('UPDATE payment_sessions SET amount = ? WHERE session_id = ?', (amount, session_id))
    conn.commit()
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
    
    session = conn.execute('''
        SELECT ps.*, 
               b.account_number as buyer_account, b.balance as buyer_balance,
               s.account_number as seller_account
        FROM payment_sessions ps
        JOIN users b ON ps.buyer_id = b.id
        JOIN users s ON ps.seller_id = s.id
        WHERE ps.session_id = ?
    ''', (session_id,)).fetchone()
    
    if not session:
        conn.close()
        return jsonify({'success': False, 'error': 'Сессия не найдена'})
    
    nfc_tag = conn.execute('SELECT n.* FROM nfc_tags n WHERE n.user_id = ? LIMIT 1', (session['buyer_id'],)).fetchone()
    
    if not nfc_tag:
        conn.close()
        return jsonify({'success': False, 'error': 'NFC-метка не найдена'})
    
    if not verify_pin(session['buyer_id'], nfc_tag['id'], pin):
        conn.close()
        return jsonify({'success': False, 'error': 'Неверный PIN-код'})
    
    amount = session['amount']
    
    if session['buyer_balance'] < amount:
        conn.close()
        return jsonify({'success': False, 'error': 'Недостаточно средств'})
    
    new_buyer_balance = session['buyer_balance'] - amount
    
    seller = conn.execute('SELECT balance FROM users WHERE id = ?', (session['seller_id'],)).fetchone()
    new_seller_balance = seller['balance'] + amount if seller else amount
    
    conn.execute('UPDATE users SET balance = ? WHERE id = ?', (new_buyer_balance, session['buyer_id']))
    conn.execute('UPDATE users SET balance = ? WHERE id = ?', (new_seller_balance, session['seller_id']))
    
    conn.execute('UPDATE payment_sessions SET status = "paid", completed_at = CURRENT_TIMESTAMP WHERE session_id = ?', (session_id,))
    
    conn.execute('''
        INSERT INTO transactions (type, from_account, to_account, amount, status, description)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', ('NFC Payment', 
          session['buyer_account'], 
          session['seller_account'],
          amount, 
          'Успешно',
          f'Оплата по NFC'))
    
    conn.commit()
    conn.close()
    
    return jsonify({
        'success': True,
        'message': f'Оплата {amount} руб. прошла успешно',
        'new_balance': new_buyer_balance
    })

@app.route('/api/nfc/status/<session_id>')
def get_payment_status(session_id):
    conn = get_db_connection()
    session = conn.execute('SELECT status, amount FROM payment_sessions WHERE session_id = ?', (session_id,)).fetchone()
    conn.close()
    
    if session:
        return jsonify(dict(session))
    return jsonify({'status': 'not_found'})

# ==================== API ДЛЯ АДМИНОВ ====================

@app.route('/admin/api/system_stats')
@require_permission('view_reports')
def admin_system_stats():
    conn = get_db_connection()
    
    today = datetime.now().strftime('%Y-%m-%d')
    today_transactions = conn.execute('SELECT COUNT(*) as count FROM transactions WHERE DATE(date) = ?', (today,)).fetchone()['count']
    
    avg_result = conn.execute('SELECT AVG(balance) as avg FROM users WHERE is_active = 1').fetchone()
    avg_balance = avg_result['avg'] if avg_result['avg'] is not None else 0
    
    turnover_result = conn.execute('SELECT COALESCE(SUM(amount), 0) as total FROM transactions WHERE DATE(date) = ? AND status = "Успешно"', (today,)).fetchone()
    total_turnover = turnover_result['total'] if turnover_result['total'] is not None else 0
    
    active_today = conn.execute('SELECT COUNT(DISTINCT user_id) as count FROM transactions WHERE DATE(date) = ?', (today,)).fetchone()['count']
    
    new_today = conn.execute('SELECT COUNT(*) as count FROM users WHERE DATE(created_at) = ?', (today,)).fetchone()['count']
    
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
    
    total_users = conn.execute('SELECT COUNT(*) as count FROM users').fetchone()['count']
    active_sessions = conn.execute('SELECT COUNT(*) as count FROM payment_sessions WHERE status = "pending"').fetchone()['count']
    
    today = datetime.now().strftime('%Y-%m-%d')
    today_transactions = conn.execute('SELECT COUNT(*) as count FROM transactions WHERE DATE(date) = ?', (today,)).fetchone()['count']
    
    total_balance = conn.execute('SELECT SUM(balance) as total FROM users').fetchone()['total'] or 0
    
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
    
    query = 'SELECT * FROM transactions WHERE 1=1'
    params = []
    
    if data.get('date_from'):
        query += ' AND DATE(date) >= ?'
        params.append(data['date_from'])
    
    if data.get('date_to'):
        query += ' AND DATE(date) <= ?'
        params.append(data['date_to'])
    
    if data.get('min_amount'):
        query += ' AND amount >= ?'
        params.append(float(data['min_amount']))
    
    if data.get('max_amount'):
        query += ' AND amount <= ?'
        params.append(float(data['max_amount']))
    
    query += ' ORDER BY date DESC LIMIT 100'
    
    transactions = conn.execute(query, params).fetchall()
    
    total_amount = sum(t['amount'] for t in transactions)
    average_amount = total_amount / len(transactions) if transactions else 0
    
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
    users = conn.execute('''
        SELECT passport, full_name, created_at, balance 
        FROM users 
        WHERE role_id = 6
        ORDER BY created_at DESC 
        LIMIT 20
    ''').fetchall()
    conn.close()
    
    return jsonify([dict(user) for user in users])

@app.route('/admin/api/admin_logs')
@require_permission('audit_logs')
def admin_admin_logs():
    conn = get_db_connection()
    
    logs = conn.execute('SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT 50').fetchall()
    
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
    
    conn.close()
    return jsonify(result)

@app.route('/admin/api/search_users')
@require_permission('view_users')
def admin_search_users():
    query = request.args.get('q', '')
    if not query:
        return jsonify([])
    
    conn = get_db_connection()
    
    users = conn.execute('''
        SELECT passport, full_name, account_number, balance, is_active, 
               CASE WHEN role_id <= 3 THEN 1 ELSE 0 END as is_admin
        FROM users 
        WHERE passport LIKE ? OR full_name LIKE ? OR account_number LIKE ?
        LIMIT 20
    ''', (f'%{query}%', f'%{query}%', f'%{query}%')).fetchall()
    
    result = []
    for user in users:
        result.append({
            'passport': user['passport'],
            'full_name': user['full_name'],
            'account_number': user['account_number'],
            'balance': user['balance'],
            'is_active': bool(user['is_active']),
            'is_admin': bool(user['is_admin'])
        })
    
    conn.close()
    return jsonify(result)

@app.route('/admin/api/user_transactions/<passport>')
@require_permission('view_transactions')
def admin_user_transactions(passport):
    conn = get_db_connection()
    
    user = conn.execute('SELECT * FROM users WHERE passport = ?', (passport,)).fetchone()
    
    if not user:
        conn.close()
        return jsonify({'error': 'Пользователь не найден'}), 404
    
    transactions = conn.execute('''
        SELECT t.*, u1.full_name as from_name, u2.full_name as to_name
        FROM transactions t
        LEFT JOIN users u1 ON t.from_account = u1.account_number
        LEFT JOIN users u2 ON t.to_account = u2.account_number
        WHERE t.from_account = ? OR t.to_account = ?
        ORDER BY t.date DESC
        LIMIT 50
    ''', (user['account_number'], user['account_number'])).fetchall()
    
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
        
        try:
            if action == 'block':
                placeholders = ','.join(['?'] * len(passports))
                conn.execute(f'UPDATE users SET is_active = 0 WHERE passport IN ({placeholders})', passports)
                flash_message = f'Заблокировано {len(passports)} пользователей'
                
            elif action == 'unblock':
                placeholders = ','.join(['?'] * len(passports))
                conn.execute(f'UPDATE users SET is_active = 1 WHERE passport IN ({placeholders})', passports)
                flash_message = f'Разблокировано {len(passports)} пользователей'
                
            elif action == 'reset_passwords':
                for passport in passports:
                    new_password = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
                    conn.execute('UPDATE users SET password_hash = ? WHERE passport = ?', 
                                (generate_password_hash(new_password), passport))
                flash_message = f'Пароли сброшены для {len(passports)} пользователей'
            else:
                conn.close()
                return jsonify({'success': False, 'error': 'Неизвестное действие'}), 400
            
            conn.commit()
            
            conn.execute('''
                INSERT INTO audit_log (admin_passport, admin_name, action, target_user, details)
                VALUES (?, ?, ?, ?, ?)
            ''', (session.get('passport'),
                  session.get('user_info', {}).get('full_name', 'Администратор'),
                  f'Групповое действие: {action}',
                  f'{len(passports)} пользователей',
                  f'Паспорта: {", ".join(passports[:5])}...'))
            conn.commit()
            
            conn.close()
            return jsonify({'success': True, 'message': flash_message})
            
        except Exception as e:
            conn.rollback()
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
    
    if request.method == 'POST':
        role_name = request.form['role_name']
        level = int(request.form['level'])
        description = request.form['description']
        
        conn.execute('UPDATE roles SET role_name = ?, level = ?, description = ? WHERE id = ?', 
                    (role_name, level, description, role_id))
        conn.commit()
        conn.close()
        
        flash('Роль обновлена', 'success')
        return redirect(url_for('admin_roles'))
    
    role = conn.execute('SELECT * FROM roles WHERE id = ?', (role_id,)).fetchone()
    conn.close()
    
    return render_template('edit_role.html', role=dict(role))

# ==================== ЗАПУСК ====================

if __name__ == '__main__':
    init_db()
    print(f"🚀 Сервер запускается на порту {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
