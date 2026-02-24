import os
import asyncpg
import asyncio
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

class Database:
    def __init__(self):
        self.pool = None
        self.connected = False
    
    async def connect(self):
        """Подключение к PostgreSQL"""
        try:
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                raise Exception("❌ DATABASE_URL не найден в переменных окружения!")
            
            print(f"📦 Подключаюсь к PostgreSQL...")
            self.pool = await asyncpg.create_pool(database_url)
            self.connected = True
            await self.create_tables()
            print("✅ PostgreSQL подключен")
            
            # Проверяем количество каналов
            async with self.pool.acquire() as conn:
                count = await conn.fetchval("SELECT COUNT(*) FROM channels")
                print(f"📊 В базе {count} каналов")
                
        except Exception as e:
            print(f"❌ Ошибка подключения к PostgreSQL: {e}")
            self.connected = False
            raise
    
    async def create_tables(self):
        """Создаем таблицы если их нет"""
        async with self.pool.acquire() as conn:
            # Каналы
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS channels (
                    id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE,
                    title TEXT,
                    description TEXT,
                    added_by BIGINT,
                    status TEXT DEFAULT 'pending',
                    subscribers INTEGER DEFAULT 0,
                    growth_7d REAL DEFAULT 0,
                    growth_30d REAL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Посты
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS posts (
                    id SERIAL PRIMARY KEY,
                    channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
                    message_id INTEGER,
                    date TIMESTAMP,
                    views INTEGER DEFAULT 0,
                    reactions INTEGER DEFAULT 0,
                    forwards INTEGER DEFAULT 0,
                    text TEXT DEFAULT '',
                    UNIQUE(channel_id, message_id)
                )
            ''')
            
            # История подписчиков
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS subscribers_history (
                    id SERIAL PRIMARY KEY,
                    channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
                    date DATE,
                    subscribers INTEGER,
                    UNIQUE(channel_id, date)
                )
            ''')
            
            print("✅ Таблицы созданы/проверены")
    
    async def add_channel(self, username: str, title: str, added_by: int) -> bool:
        """Добавить канал на модерацию"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO channels (username, title, added_by, status)
                    VALUES ($1, $2, $3, 'pending')
                    ON CONFLICT (username) DO NOTHING
                ''', username, title, added_by)
                return True
        except Exception as e:
            print(f"❌ Ошибка добавления канала: {e}")
            return False
    
    async def approve_channel(self, channel_id: int) -> bool:
        """Одобрить канал"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    UPDATE channels SET status = 'approved' WHERE id = $1
                ''', channel_id)
                return True
        except Exception as e:
            print(f"❌ Ошибка одобрения: {e}")
            return False
    
    async def reject_channel(self, channel_id: int) -> bool:
        """Отклонить канал"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    UPDATE channels SET status = 'rejected' WHERE id = $1
                ''', channel_id)
                return True
        except Exception as e:
            print(f"❌ Ошибка отклонения: {e}")
            return False
    
    async def delete_channel(self, channel_id: int) -> bool:
        """Удалить канал полностью"""
        try:
            async with self.pool.acquire() as conn:
                # Посты удалятся автоматически (ON DELETE CASCADE)
                await conn.execute('''
                    DELETE FROM channels WHERE id = $1
                ''', channel_id)
                print(f"✅ Канал {channel_id} удален")
                return True
        except Exception as e:
            print(f"❌ Ошибка удаления: {e}")
            return False
    
    async def get_pending_channels(self) -> List[Tuple]:
        """Получить каналы на модерации"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT id, username, title, added_by, created_at 
                FROM channels 
                WHERE status = 'pending'
                ORDER BY created_at DESC
            ''')
            return [(r['id'], r['username'], r['title'], r['added_by'], r['created_at']) for r in rows]
    
    async def get_all_approved_channels(self) -> List[Tuple]:
        """Все одобренные каналы"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT id, username, title FROM channels 
                WHERE status = 'approved'
                ORDER BY created_at DESC
            ''')
            return [(r['id'], r['username'], r['title']) for r in rows]
    
    async def get_all_channels(self) -> List[Tuple]:
        """Все каналы (для админа)"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT id, username, title, status, subscribers 
                FROM channels 
                ORDER BY created_at DESC
            ''')
            return [(r['id'], r['username'], r['title'], r['status'], r['subscribers']) for r in rows]
    
    async def update_channel_stats(self, channel_id: int, subscribers: int) -> Tuple[float, float]:
        """Обновить статистику канала"""
        try:
            async with self.pool.acquire() as conn:
                now = datetime.now().date()
                
                # Сохраняем историю
                await conn.execute('''
                    INSERT INTO subscribers_history (channel_id, date, subscribers)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (channel_id, date) DO UPDATE SET subscribers = $3
                ''', channel_id, now.isoformat(), subscribers)
                
                # Считаем рост за 7 дней
                week_ago = (now - timedelta(days=7)).isoformat()
                week_old = await conn.fetchval('''
                    SELECT subscribers FROM subscribers_history 
                    WHERE channel_id=$1 AND date=$2
                ''', channel_id, week_ago)
                
                growth_7d = 0
                if week_old and week_old > 0:
                    growth_7d = round(((subscribers - week_old) / week_old) * 100, 1)
                
                # Считаем рост за 30 дней
                month_ago = (now - timedelta(days=30)).isoformat()
                month_old = await conn.fetchval('''
                    SELECT subscribers FROM subscribers_history 
                    WHERE channel_id=$1 AND date=$2
                ''', channel_id, month_ago)
                
                growth_30d = 0
                if month_old and month_old > 0:
                    growth_30d = round(((subscribers - month_old) / month_old) * 100, 1)
                
                # Обновляем канал
                await conn.execute('''
                    UPDATE channels 
                    SET subscribers=$1, growth_7d=$2, growth_30d=$3, updated_at=CURRENT_TIMESTAMP
                    WHERE id=$4
                ''', subscribers, growth_7d, growth_30d, channel_id)
                
                return growth_7d, growth_30d
                
        except Exception as e:
            print(f"❌ Ошибка обновления статистики: {e}")
            return 0, 0
    
    async def add_post(self, channel_id: int, message_id: int, date, views=0, reactions=0, forwards=0, text='') -> bool:
        """Добавить или обновить пост"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO posts (channel_id, message_id, date, views, reactions, forwards, text)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (channel_id, message_id) DO UPDATE 
                    SET views=$4, reactions=$5, forwards=$6, text=$7
                ''', channel_id, message_id, date, views, reactions, forwards, text)
                return True
        except Exception as e:
            print(f"❌ Ошибка добавления поста: {e}")
            return False
    
    async def get_post_text(self, channel_id: int, message_id: int) -> str:
        """Получить текст поста"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchval('''
                SELECT text FROM posts 
                WHERE channel_id=$1 AND message_id=$2
            ''', channel_id, message_id)
            return result or ''
    
    # ========== ТОПЫ ==========
    
    async def get_top_posts_by_reactions(self, limit=20) -> List[Tuple]:
        """Топ постов по реакциям за последние 7 дней"""
        async with self.pool.acquire() as conn:
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            
            rows = await conn.fetch('''
                SELECT p.channel_id, c.username, c.title, p.message_id, p.reactions, p.date, p.text
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE c.status='approved' AND p.date >= $1 AND p.reactions > 0
                ORDER BY p.reactions DESC
                LIMIT $2
            ''', week_ago, limit)
            
            return [(r['channel_id'], r['username'], r['title'], r['message_id'], 
                    r['reactions'], r['date'], r['text']) for r in rows]
    
    async def get_top_posts_by_views(self, limit=20) -> List[Tuple]:
        """Топ постов по просмотрам за последние 7 дней"""
        async with self.pool.acquire() as conn:
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            
            rows = await conn.fetch('''
                SELECT p.channel_id, c.username, c.title, p.message_id, p.views, p.date, p.text
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE c.status='approved' AND p.date >= $1 AND p.views > 0
                ORDER BY p.views DESC
                LIMIT $2
            ''', week_ago, limit)
            
            return [(r['channel_id'], r['username'], r['title'], r['message_id'], 
                    r['views'], r['date'], r['text']) for r in rows]
    
    async def get_top_posts_by_forwards(self, limit=20) -> List[Tuple]:
        """Топ постов по репостам за последние 7 дней"""
        async with self.pool.acquire() as conn:
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            
            rows = await conn.fetch('''
                SELECT p.channel_id, c.username, c.title, p.message_id, p.forwards, p.date, p.text
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE c.status='approved' AND p.date >= $1 AND p.forwards > 0
                ORDER BY p.forwards DESC
                LIMIT $2
            ''', week_ago, limit)
            
            return [(r['channel_id'], r['username'], r['title'], r['message_id'], 
                    r['forwards'], r['date'], r['text']) for r in rows]
    
    async def get_top_channels_by_growth(self, period='7d', limit=20) -> List[Tuple]:
        """Топ каналов по росту"""
        async with self.pool.acquire() as conn:
            if period == '7d':
                rows = await conn.fetch('''
                    SELECT id, username, title, subscribers, growth_7d, growth_30d
                    FROM channels 
                    WHERE status='approved' AND subscribers >= 100
                    ORDER BY growth_7d DESC
                    LIMIT $1
                ''', limit)
            else:
                rows = await conn.fetch('''
                    SELECT id, username, title, subscribers, growth_7d, growth_30d
                    FROM channels 
                    WHERE status='approved' AND subscribers >= 100
                    ORDER BY growth_30d DESC
                    LIMIT $1
                ''', limit)
            
            return [(r['id'], r['username'], r['title'], r['subscribers'], 
                    r['growth_7d'], r['growth_30d']) for r in rows]
    
    async def get_top_posts_small_channels(self, limit=20) -> List[Tuple]:
        """Топ постов для каналов с менее 3000 подписчиков за последние 7 дней"""
        async with self.pool.acquire() as conn:
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            
            rows = await conn.fetch('''
                SELECT p.channel_id, c.username, c.title, p.message_id, p.views, p.date, p.text
                FROM posts p
                JOIN channels c ON p.channel_id = c.id
                WHERE c.status='approved' 
                AND c.subscribers < 3000 
                AND p.date >= $1 
                AND p.views > 0
                ORDER BY p.views DESC
                LIMIT $2
            ''', week_ago, limit)
            
            return [(r['channel_id'], r['username'], r['title'], r['message_id'], 
                    r['views'], r['date'], r['text']) for r in rows]
    
    async def get_channel(self, channel_id: int) -> Optional[Tuple]:
        """Получить канал по ID"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM channels WHERE id=$1', channel_id)
            if row:
                return (row['id'], row['username'], row['title'], row['description'],
                       row['added_by'], row['status'], row['subscribers'], row['growth_7d'],
                       row['growth_30d'], row['created_at'], row['updated_at'])
            return None
    
    async def get_channel_by_username(self, username: str) -> Optional[Tuple]:
        """Получить канал по username"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM channels WHERE username=$1', username)
            if row:
                return (row['id'], row['username'], row['title'], row['description'],
                       row['added_by'], row['status'], row['subscribers'], row['growth_7d'],
                       row['growth_30d'], row['created_at'], row['updated_at'])
            return None
    
    async def get_user_channels_count(self, user_id: int) -> int:
        """Сколько каналов добавил пользователь"""
        async with self.pool.acquire() as conn:
            count = await conn.fetchval('''
                SELECT COUNT(*) FROM channels WHERE added_by=$1
            ''', user_id)
            return count or 0
    
    async def get_channel_posts_count(self, channel_id: int) -> int:
        """Количество постов у канала"""
        async with self.pool.acquire() as conn:
            count = await conn.fetchval('''
                SELECT COUNT(*) FROM posts WHERE channel_id=$1
            ''', channel_id)
            return count or 0
    
    async def close(self):
        """Закрыть соединение"""
        if self.pool:
            await self.pool.close()
            print("🔌 Соединение с PostgreSQL закрыто")
