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
        """Подключение к Telegram"""
        try:
            # Если уже подключены, возвращаем True
            if self.client and self.connected:
                return True
            
            # Если клиент существует но не подключен, пробуем переподключиться
            if self.client and not self.connected:
                try:
                    await self.client.connect()
                    if await self.client.is_user_authorized():
                        self.connected = True
                        print("✅ Telethon переподключен")
                        return True
                except:
                    pass
            
            print(f"🔗 Подключаю Telethon...")
            
            # Создаем нового клиента
            self.client = TelegramClient(
                'parser_session',
                config.API_ID,
                config.API_HASH,
                connection_retries=10,
                timeout=30
            )
            
            await self.client.start()
            self.connected = True
            print("✅ Telethon подключен")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка подключения Telethon: {e}")
            self.connected = False
            return False
    
    async def ensure_connected(self):
        """Проверяет подключение и переподключает если нужно"""
        try:
            if not self.client or not self.connected:
                return await self.connect()
            
            # Проверяем, работает ли подключение
            try:
                await self.client.get_me()
                return True
            except:
                self.connected = False
                return await self.connect()
                
        except Exception as e:
            print(f"❌ Ошибка проверки подключения: {e}")
            self.connected = False
            return await self.connect()
    
    async def close(self):
        """Закрыть соединение"""
        if self.client:
            try:
                await self.client.disconnect()
            except:
                pass
            self.connected = False
            print("🔌 Telethon отключен")
    
    async def get_channel_info(self, username):
        """Получить информацию о канале"""
        try:
            # Проверяем подключение перед каждым запросом
            if not await self.ensure_connected():
                print(f"❌ Нет подключения к Telegram")
                return None
            
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
    
    async def get_channel_posts_last_week(self, username):
        """Получить посты из канала за последние 7 дней"""
        try:
            # Проверяем подключение перед каждым запросом
            if not await self.ensure_connected():
                print(f"❌ Нет подключения к Telegram")
                return []
            
            if not username.startswith('@'):
                username = '@' + username
            
            entity = await self.client.get_entity(username)
            
            # Вычисляем дату 7 дней назад
            week_ago = datetime.now() - timedelta(days=7)
            
            posts = []
            post_count = 0
            
            print(f"📅 Собираю посты за последние 7 дней для {username}...")
            
            # Собираем посты за последние 7 дней
            async for message in self.client.iter_messages(entity, offset_date=datetime.now(), reverse=False):
                if message is None or not hasattr(message, 'id'):
                    continue
                
                # Проверяем, что пост не старше 7 дней
                if message.date.replace(tzinfo=None) < week_ago:
                    break
                
                post_count += 1
                
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
                
                posts.append({
                    'message_id': message.id,
                    'date': message.date,
                    'views': views,
                    'reactions': reaction_count,
                    'forwards': getattr(message, 'forwards', 0),
                    'text': message_text
                })
            
            print(f"📊 Собрано {post_count} постов за последние 7 дней для {username}")
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
            
            # Используем новую функцию для получения постов за последние 7 дней
            posts = await self.get_channel_posts_last_week(username)
            
            saved_count = 0
            for post in posts:
                if db.add_post(
                    channel_id=channel_id,
                    message_id=post['message_id'],
                    date=post['date'],
                    views=post['views'],
                    reactions=post['reactions'],
                    forwards=post['forwards'],
                    text=post['text']
                ):
                    saved_count += 1
            
            print(f"✅ Обновлен {username}: {info['subscribers']} подписчиков, сохранено {saved_count} постов за 7 дней")
            
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
        
        # Проверяем подключение
        if not await self.ensure_connected():
            print("❌ Не удалось подключиться к Telegram")
            return []
        
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
