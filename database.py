import sqlite3
import threading
from datetime import datetime, timedelta

class Database:
    def __init__(self):
        self.lock = threading.Lock()
        self.conn = sqlite3.connect('christian_catalog.db', check_same_thread=False, timeout=10)
        self.cursor = self.conn.cursor()
        self.create_tables()
        self.migrate_posts_table()
    
    def create_tables(self):
        """Создаем таблицы"""
        with self.lock:
            # Каналы
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE,
                    title TEXT,
                    description TEXT,
                    added_by INTEGER,
                    status TEXT DEFAULT 'pending',
                    subscribers INTEGER DEFAULT 0,
                    growth_7d REAL DEFAULT 0,
                    growth_30d REAL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Посты
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id INTEGER,
                    message_id INTEGER,
                    date TIMESTAMP,
                    views INTEGER DEFAULT 0,
                    reactions INTEGER DEFAULT 0,
                    forwards INTEGER DEFAULT 0,
                    text TEXT DEFAULT '',
                    FOREIGN KEY (channel_id) REFERENCES channels(id),
                    UNIQUE(channel_id, message_id)
                )
            ''')
            
            # История подписчиков
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS subscribers_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id INTEGER,
                    date DATE,
                    subscribers INTEGER,
                    FOREIGN KEY (channel_id) REFERENCES channels(id),
                    UNIQUE(channel_id, date)
                )
            ''')
            
            self.conn.commit()
    
    def migrate_posts_table(self):
        """Миграция: добавляем поле text в таблицу posts, если его нет"""
        with self.lock:
            try:
                self.cursor.execute("PRAGMA table_info(posts)")
                columns = [column[1] for column in self.cursor.fetchall()]
                
                if 'text' not in columns:
                    print("🔄 Миграция: добавляем поле text в таблицу posts...")
                    self.cursor.execute("ALTER TABLE posts ADD COLUMN text TEXT DEFAULT ''")
                    self.conn.commit()
                    print("✅ Миграция завершена")
            except Exception as e:
                print(f"❌ Ошибка миграции: {e}")
    
    def add_channel(self, username: str, title: str, added_by: int):
        """Добавить канал на модерацию"""
        with self.lock:
            try:
                self.cursor.execute('''
                    INSERT OR IGNORE INTO channels (username, title, added_by, status)
                    VALUES (?, ?, ?, 'pending')
                ''', (username, title, added_by))
                self.conn.commit()
                return True
            except:
                return False
    
    def approve_channel(self, channel_id: int):
        """Одобрить канал"""
        with self.lock:
            try:
                self.cursor.execute('UPDATE channels SET status = "approved" WHERE id = ?', (channel_id,))
                self.conn.commit()
                return True
            except:
                return False
    
    def reject_channel(self, channel_id: int):
        """Отклонить канал"""
        with self.lock:
            try:
                self.cursor.execute('UPDATE channels SET status = "rejected" WHERE id = ?', (channel_id,))
                self.conn.commit()
                return True
            except:
                return False
    
    def delete_channel(self, channel_id: int):
        """Удалить канал полностью"""
        with self.lock:
            try:
                self.cursor.execute('DELETE FROM posts WHERE channel_id = ?', (channel_id,))
                self.cursor.execute('DELETE FROM subscribers_history WHERE channel_id = ?', (channel_id,))
                self.cursor.execute('DELETE FROM channels WHERE id = ?', (channel_id,))
                self.conn.commit()
                return True
            except Exception as e:
                print(f"❌ Ошибка удаления: {e}")
                return False
    
    def get_pending_channels(self):
        """Получить каналы на модерации"""
        with self.lock:
            self.cursor.execute('''
                SELECT id, username, title, added_by, created_at 
                FROM channels 
                WHERE status = 'pending'
            ''')
            return self.cursor.fetchall()
    
    def get_all_approved_channels(self):
        """Все одобренные каналы"""
        with self.lock:
            self.cursor.execute('SELECT id, username, title FROM channels WHERE status = "approved"')
            return self.cursor.fetchall()
    
    def get_all_channels(self):
        """Все каналы (для админа)"""
        with self.lock:
            self.cursor.execute('SELECT id, username, title, status, subscribers FROM channels ORDER BY created_at DESC')
            return self.cursor.fetchall()
    
    def update_channel_stats(self, channel_id: int, subscribers: int):
        """Обновить статистику канала"""
        with self.lock:
            try:
                now = datetime.now().date()
                
                self.cursor.execute('''
                    INSERT OR REPLACE INTO subscribers_history (channel_id, date, subscribers)
                    VALUES (?, ?, ?)
                ''', (channel_id, now.isoformat(), subscribers))
                
                week_ago = (now - timedelta(days=7)).isoformat()
                self.cursor.execute('SELECT subscribers FROM subscribers_history WHERE channel_id=? AND date=?', 
                                  (channel_id, week_ago))
                week_old = self.cursor.fetchone()
                
                growth_7d = 0
                if week_old and week_old[0] > 0:
                    growth_7d = round(((subscribers - week_old[0]) / week_old[0]) * 100, 1)
                
                month_ago = (now - timedelta(days=30)).isoformat()
                self.cursor.execute('SELECT subscribers FROM subscribers_history WHERE channel_id=? AND date=?', 
                                  (channel_id, month_ago))
                month_old = self.cursor.fetchone()
                
                growth_30d = 0
                if month_old and month_old[0] > 0:
                    growth_30d = round(((subscribers - month_old[0]) / month_old[0]) * 100, 1)
                
                self.cursor.execute('''
                    UPDATE channels 
                    SET subscribers=?, growth_7d=?, growth_30d=?, updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                ''', (subscribers, growth_7d, growth_30d, channel_id))
                
                self.conn.commit()
                return growth_7d, growth_30d
            except Exception as e:
                print(f"❌ Ошибка обновления статистики: {e}")
                return 0, 0
    
    def add_post(self, channel_id: int, message_id: int, date, views=0, reactions=0, forwards=0, text=''):
        """Добавить или обновить пост"""
        with self.lock:
            try:
                # Проверяем, существует ли уже такой пост
                self.cursor.execute('''
                    SELECT id FROM posts WHERE channel_id=? AND message_id=?
                ''', (channel_id, message_id))
                existing = self.cursor.fetchone()
                
                if existing:
                    # Обновляем существующий пост
                    self.cursor.execute('''
                        UPDATE posts 
                        SET views=?, reactions=?, forwards=?, text=?
                        WHERE channel_id=? AND message_id=?
                    ''', (views, reactions, forwards, text, channel_id, message_id))
                else:
                    # Вставляем новый пост
                    self.cursor.execute('''
                        INSERT INTO posts (channel_id, message_id, date, views, reactions, forwards, text)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (channel_id, message_id, date, views, reactions, forwards, text))
                
                self.conn.commit()
                return True
            except Exception as e:
                print(f"❌ Ошибка добавления поста: {e}")
                return False
    
    def get_post_text(self, channel_id: int, message_id: int):
        """Получить текст поста"""
        with self.lock:
            self.cursor.execute('SELECT text FROM posts WHERE channel_id=? AND message_id=?', 
                              (channel_id, message_id))
            result = self.cursor.fetchone()
            return result[0] if result else ''
    
    # ========== ТОПЫ С ФИЛЬТРАЦИЕЙ ПО ДАТЕ ==========
    
    def get_top_posts_by_reactions(self, limit=20):
        """Топ постов по реакциям за последние 7 дней"""
        with self.lock:
            # Посты за последние 7 дней
            week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
            
            self.cursor.execute('''
                SELECT p.channel_id, c.username, c.title, p.message_id, p.reactions, p.date, p.text
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE c.status='approved' AND p.date >= ? AND p.reactions > 0
                ORDER BY p.reactions DESC
                LIMIT ?
            ''', (week_ago, limit))
            return self.cursor.fetchall()
    
    def get_top_posts_by_views(self, limit=20):
        """Топ постов по просмотрам за последние 7 дней"""
        with self.lock:
            # Посты за последние 7 дней
            week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
            
            self.cursor.execute('''
                SELECT p.channel_id, c.username, c.title, p.message_id, p.views, p.date, p.text
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE c.status='approved' AND p.date >= ? AND p.views > 0
                ORDER BY p.views DESC
                LIMIT ?
            ''', (week_ago, limit))
            return self.cursor.fetchall()
    
    def get_top_posts_by_forwards(self, limit=20):
        """Топ постов по репостам за последние 7 дней"""
        with self.lock:
            # Посты за последние 7 дней
            week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
            
            self.cursor.execute('''
                SELECT p.channel_id, c.username, c.title, p.message_id, p.forwards, p.date, p.text
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE c.status='approved' AND p.date >= ? AND p.forwards > 0
                ORDER BY p.forwards DESC
                LIMIT ?
            ''', (week_ago, limit))
            return self.cursor.fetchall()
    
    def get_top_channels_by_growth(self, period='7d', limit=20):
        """Топ каналов по росту (за 30 дней для месячной статистики)"""
        with self.lock:
            if period == '7d':
                order_by = 'growth_7d DESC'
            else:
                order_by = 'growth_30d DESC'  # рост за 30 дней
            
            self.cursor.execute(f'''
                SELECT id, username, title, subscribers, growth_7d, growth_30d
                FROM channels 
                WHERE status='approved' AND subscribers >= 100
                ORDER BY {order_by}
                LIMIT ?
            ''', (limit,))
            return self.cursor.fetchall()
    
    def get_top_posts_small_channels(self, limit=20):
        """Топ постов для каналов с менее 3000 подписчиков за последние 7 дней"""
        with self.lock:
            # Посты за последние 7 дней
            week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
            
            self.cursor.execute('''
                SELECT p.channel_id, c.username, c.title, p.message_id, p.views, p.date, p.text
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE c.status='approved' 
                AND c.subscribers < 3000 
                AND p.date >= ? 
                AND p.views > 0
                ORDER BY p.views DESC
                LIMIT ?
            ''', (week_ago, limit))
            return self.cursor.fetchall()
    
    def get_channel(self, channel_id: int):
        """Получить канал по ID"""
        with self.lock:
            self.cursor.execute('SELECT * FROM channels WHERE id=?', (channel_id,))
            return self.cursor.fetchone()
    
    def get_channel_by_username(self, username: str):
        """Получить канал по username"""
        with self.lock:
            self.cursor.execute('SELECT * FROM channels WHERE username=?', (username,))
            return self.cursor.fetchone()
    
    def get_user_channels_count(self, user_id: int):
        """Сколько каналов добавил пользователь"""
        with self.lock:
            self.cursor.execute('SELECT COUNT(*) FROM channels WHERE added_by=?', (user_id,))
            result = self.cursor.fetchone()
            return result[0] if result else 0
    
    def get_channel_posts_count(self, channel_id: int):
        """Количество постов у канала"""
        with self.lock:
            self.cursor.execute('SELECT COUNT(*) FROM posts WHERE channel_id=?', (channel_id,))
            result = self.cursor.fetchone()
            return result[0] if result else 0
    
    def close(self):
        """Закрыть соединение с базой данных"""
        with self.lock:
            if self.conn:
                self.conn.close()
                print("🔌 Соединение с БД закрыто")
