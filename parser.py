import asyncio
from datetime import datetime, timedelta
from telethon import TelegramClient, errors
from telethon.tl.functions.channels import GetFullChannelRequest
import config

class TelegramParser:
    def __init__(self):
        self.client = None
        self.connected = False
    
    async def connect(self):
        """Подключение к Telegram - БЕЗ ПРОКСИ"""
        try:
            if self.connected and self.client:
                return True
            
            print(f"🔗 Подключаю Telethon...")
            
            self.client = TelegramClient(
                'parser_session',
                config.API_ID,
                config.API_HASH,
                connection_retries=5,
                timeout=30
                # ПРОКСИ УДАЛЕН!
            )
            
            await self.client.start()
            self.connected = True
            print("✅ Telethon подключен")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка подключения Telethon: {e}")
            return False
    
    async def close(self):
        """Закрыть соединение"""
        if self.client and self.connected:
            await self.client.disconnect()
            self.connected = False
            print("🔌 Telethon отключен")
    
    async def get_channel_info(self, username):
        """Получить информацию о канале"""
        try:
            if not username.startswith('@'):
                username = '@' + username
            
            print(f"🔍 Получаю данные {username}")
            
            entity = await self.client.get_entity(username)
            
            try:
                full = await self.client(GetFullChannelRequest(channel=entity))
                subscribers = full.full_chat.participants_count
            except:
                subscribers = 0
            
            return {
                'id': entity.id,
                'username': entity.username if hasattr(entity, 'username') else username,
                'title': entity.title,
                'description': getattr(entity, 'about', ''),
                'subscribers': subscribers,
                'date': datetime.now()
            }
            
        except errors.UsernameInvalidError:
            print(f"❌ Неверный username: {username}")
            return None
        except errors.ChannelPrivateError:
            print(f"❌ Канал {username} приватный")
            return None
        except errors.FloodWaitError as e:
            print(f"⚠️ Флуд-вейт: {e.seconds} секунд")
            await asyncio.sleep(e.seconds)
            return None
        except Exception as e:
            print(f"❌ Ошибка получения {username}: {e}")
            return None
    
    async def get_channel_posts(self, username, limit=30):
        """Получить посты из канала"""
        try:
            if not username.startswith('@'):
                username = '@' + username
            
            entity = await self.client.get_entity(username)
            
            posts = []
            async for message in self.client.iter_messages(entity, limit=limit):
                if message is None or not hasattr(message, 'id'):
                    continue
                
                # Получаем текст сообщения
                message_text = ""
                if hasattr(message, 'message') and message.message:
                    message_text = message.message
                elif hasattr(message, 'text') and message.text:
                    message_text = message.text
                
                # Считаем реакции
                reaction_count = 0
                if hasattr(message, 'reactions') and message.reactions:
                    if hasattr(message.reactions, 'results'):
                        for reaction in message.reactions.results:
                            reaction_count += reaction.count
                    elif hasattr(message.reactions, 'recent_reactions'):
                        reaction_count = len(message.reactions.recent_reactions)
                
                views = getattr(message, 'views', 0)
                if views == 0 and reaction_count == 0 and not message_text:
                    continue
                
                posts.append({
                    'message_id': message.id,
                    'date': message.date,
                    'views': views,
                    'reactions': reaction_count,
                    'forwards': getattr(message, 'forwards', 0),
                    'text': message_text  # ДОБАВЛЯЕМ ТЕКСТ!
                })
            
            return posts
            
        except Exception as e:
            print(f"❌ Ошибка постов {username}: {e}")
            return []
    
    async def update_channel_stats(self, username, db):
        """Обновить статистику канала"""
        try:
            info = await self.get_channel_info(username)
            if not info:
                return None
            
            channel = db.get_channel_by_username(username)
            if not channel:
                print(f"❌ Канал {username} не найден в базе")
                return None
            
            channel_id = channel[0]
            
            growth_7d, growth_30d = db.update_channel_stats(channel_id, info['subscribers'])
            
            posts = await self.get_channel_posts(username, limit=config.POSTS_LIMIT)
            
            saved_count = 0
            for post in posts:
                if db.add_post(
                    channel_id=channel_id,
                    message_id=post['message_id'],
                    date=post['date'],
                    views=post['views'],
                    reactions=post['reactions'],
                    forwards=post['forwards'],
                    text=post['text']  # ПЕРЕДАЕМ ТЕКСТ!
                ):
                    saved_count += 1
            
            print(f"✅ Обновлен {username}: {info['subscribers']} подписчиков, {saved_count} постов")
            
            return {
                'username': info['username'],
                'title': info['title'],
                'subscribers': info['subscribers'],
                'posts': saved_count,
                'growth_7d': growth_7d,
                'growth_30d': growth_30d
            }
            
        except Exception as e:
            print(f"❌ Ошибка обновления {username}: {e}")
            return None
    
    async def update_all_channels(self, db):
        """Обновить все каналы"""
        print("🔄 Начинаю обновление всех каналов...")
        
        channels = db.get_all_approved_channels()
        if not channels:
            print("📭 Нет одобренных каналов")
            return []
        
        results = []
        for channel_id, username, title in channels:
            print(f"📊 Обновляю {title}...")
            
            result = await self.update_channel_stats(username, db)
            if result:
                results.append(result)
            
            await asyncio.sleep(3)  # Пауза между запросами
        
        print(f"✅ Обновлено {len(results)} каналов")
        return results
