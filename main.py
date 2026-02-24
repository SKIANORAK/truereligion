import asyncio
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode

import config
import database
import parser
import pytz 
import os

# ========== ИНИЦИАЛИЗАЦИЯ ==========
bot = Bot(token=config.BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

db = database.Database()
telegram_parser = parser.TelegramParser()

# ID канала для отчетов
REPORT_CHANNEL_ID = config.REPORT_CHANNEL_ID

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def escape_markdown(text: str) -> str:
    """Экранирует специальные символы для Markdown"""
    if not text:
        return ""
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

def get_title_from_text(text: str, word_limit: int = 15) -> str:
    """
    Берет первые word_limit слов из текста.
    Слова разделяются пробелами, знаки препинания не считаются отдельными словами.
    """
    if not text or not isinstance(text, str) or text.strip() == "":
        return "Нет текста"
    
    words = text.strip().split()
    
    if not words:
        return "Нет текста"
    
    first_words = words[:word_limit]
    title = ' '.join(first_words)
    
    if len(words) > word_limit:
        title += "..."
    
    return escape_markdown(title)

def format_number(num: int) -> str:
    """Форматирование чисел (1000 -> 1K)"""
    if num >= 1000000:
        return f"{num/1000000:.1f}M".replace('.0M', 'M')
    if num >= 1000:
        return f"{num/1000:.1f}K".replace('.0K', 'K')
    return str(num)

# ========== STATES ==========
class ChannelStates(StatesGroup):
    waiting_link = State()

# ========== КЛАВИАТУРЫ ==========
def get_main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📈 Топ посты по реакциям", callback_data="top_reactions")
    kb.button(text="👁️ Топ посты по просмотрам", callback_data="top_views")
    kb.button(text="🔄 Топ посты по репостам", callback_data="top_forwards")
    kb.button(text="🚀 Топ каналы по росту", callback_data="top_growth")
    kb.button(text="📊 Топ малые каналы (<3K)", callback_data="top_small")
    kb.button(text="ℹ️ О проекте", callback_data="about")
    kb.button(text="➕ Добавить канал", callback_data="add_channel")
    kb.adjust(2, 2, 2, 1)
    return kb.as_markup()

def get_back_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 В главное меню", callback_data="main_menu")
    return kb.as_markup()

def get_growth_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📅 За 7 дней", callback_data="growth_7d")
    kb.button(text="📆 За 30 дней", callback_data="growth_30d")
    kb.button(text="⬅️ Назад", callback_data="main_menu")
    kb.adjust(2, 1)
    return kb.as_markup()

def get_cancel_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data="cancel_add_channel")
    return kb.as_markup()

# ========== ОБРАБОТЧИКИ ==========
@dp.message(CommandStart())
async def start_handler(message: Message):
    """Главное меню"""
    username = message.from_user.username or message.from_user.first_name
    
    text = f"""👋 Привет, {username}!

🤖 Christian Channels Catalog

📊 Реальные топы христианских каналов:
• Топ посты по реакциям (15 лучших)
• Топ посты по просмотрам (15 лучших)  
• Топ посты по репостам (15 лучших)
• Топ каналы по росту (15 лучших)
• Топ малые каналы (15 лучших, <3000 подписчиков)

🎯 Выбери раздел:"""
    
    await message.answer(text, reply_markup=get_main_menu())

@dp.callback_query(F.data == "main_menu")
async def main_menu_handler(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()
    
    username = callback.from_user.username or callback.from_user.first_name
    
    text = f"""👋 Привет, {username}!

🤖 Christian Channels Catalog

📊 Реальные топы христианских каналов:
• Топ посты по реакциям (15 лучших)
• Топ посты по просмотрам (15 лучших)  
• Топ посты по репостам (15 лучших)
• Топ каналы по росту (15 лучших)
• Топ малые каналы (15 лучших, <3000 подписчиков)

🎯 Выбери раздел:"""
    
    if callback.message.text != text:
        await callback.message.edit_text(text, reply_markup=get_main_menu())
    
    await callback.answer()

@dp.callback_query(F.data == "cancel_add_channel")
async def cancel_add_channel(callback: CallbackQuery, state: FSMContext):
    """Отмена добавления канала"""
    await state.clear()
    await main_menu_handler(callback, state)

# ========== ТОП ПОСТОВ ПО РЕАКЦИЯМ ==========
@dp.callback_query(F.data == "top_reactions")
async def top_reactions_handler(callback: CallbackQuery):
    """Топ постов по реакциям (15 позиций)"""
    posts = await db.get_top_posts_by_reactions(15)
    
    if not posts:
        text = "📭 Пока нет данных о постах с реакциями.\n\nДобавленные каналы обновляются каждые 30 минут."
        if callback.message.text != text:
            await callback.message.edit_text(text, reply_markup=get_main_menu())
        await callback.answer()
        return
    
    text = "🏆 Топ-15 постов по реакциям:\n\n"
    kb = InlineKeyboardBuilder()
    
    for idx, (channel_id, username, title, message_id, reactions, post_date, post_text) in enumerate(posts, 1):
        date_str = post_date.strftime('%d.%m') if hasattr(post_date, 'strftime') else str(post_date)[:10]
        preview = get_title_from_text(post_text, 7)
        
        text += f"{idx}. {title}\n"
        if preview and preview != "Нет текста":
            text += f"   💬 {preview}\n"
        text += f"   ❤️ {reactions} реакций | {date_str}\n"
        
        btn_text = f"#{idx} {title[:15]}"
        if len(title) > 15:
            btn_text += "..."
        
        kb.button(text=btn_text, callback_data=f"post_{channel_id}_{message_id}")
    
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1)
    
    new_markup = kb.as_markup()
    if callback.message.text != text or callback.message.reply_markup != new_markup:
        await callback.message.edit_text(text, reply_markup=new_markup)
    
    await callback.answer()

# ========== ТОП ПОСТОВ ПО ПРОСМОТРАМ ==========
@dp.callback_query(F.data == "top_views")
async def top_views_handler(callback: CallbackQuery):
    """Топ постов по просмотрам (15 позиций)"""
    posts = await db.get_top_posts_by_views(15)
    
    if not posts:
        text = "📭 Пока нет данных о постах с просмотрами.\n\nДобавленные каналы обновляются каждые 30 минут."
        if callback.message.text != text:
            await callback.message.edit_text(text, reply_markup=get_main_menu())
        await callback.answer()
        return
    
    text = "🏆 Топ-15 постов по просмотрам:\n\n"
    kb = InlineKeyboardBuilder()
    
    for idx, (channel_id, username, title, message_id, views, post_date, post_text) in enumerate(posts, 1):
        date_str = post_date.strftime('%d.%m') if hasattr(post_date, 'strftime') else str(post_date)[:10]
        views_formatted = format_number(views)
        preview = get_title_from_text(post_text, 7)
        
        text += f"{idx}. {title}\n"
        if preview and preview != "Нет текста":
            text += f"   💬 {preview}\n"
        text += f"   👁️ {views_formatted} просмотров | {date_str}\n"
        
        btn_text = f"#{idx} {title[:15]}"
        if len(title) > 15:
            btn_text += "..."
        
        kb.button(text=btn_text, callback_data=f"post_{channel_id}_{message_id}")
    
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1)
    
    new_markup = kb.as_markup()
    if callback.message.text != text or callback.message.reply_markup != new_markup:
        await callback.message.edit_text(text, reply_markup=new_markup)
    
    await callback.answer()

# ========== ТОП ПОСТОВ ПО РЕПОСТАМ ==========
@dp.callback_query(F.data == "top_forwards")
async def top_forwards_handler(callback: CallbackQuery):
    """Топ постов по репостам (15 позиций)"""
    posts = await db.get_top_posts_by_forwards(15)
    
    if not posts:
        text = "📭 Пока нет данных о постах с репостами.\n\nДобавленные каналы обновляются каждые 30 минут."
        if callback.message.text != text:
            await callback.message.edit_text(text, reply_markup=get_main_menu())
        await callback.answer()
        return
    
    text = "🏆 Топ-15 постов по репостам:\n\n"
    kb = InlineKeyboardBuilder()
    
    for idx, (channel_id, username, title, message_id, forwards, post_date, post_text) in enumerate(posts, 1):
        date_str = post_date.strftime('%d.%m') if hasattr(post_date, 'strftime') else str(post_date)[:10]
        preview = get_title_from_text(post_text, 7)
        
        text += f"{idx}. {title}\n"
        if preview and preview != "Нет текста":
            text += f"   💬 {preview}\n"
        text += f"   🔄 {forwards} репостов | {date_str}\n"
        
        btn_text = f"#{idx} {title[:15]}"
        if len(title) > 15:
            btn_text += "..."
        
        kb.button(text=btn_text, callback_data=f"post_{channel_id}_{message_id}")
    
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1)
    
    new_markup = kb.as_markup()
    if callback.message.text != text or callback.message.reply_markup != new_markup:
        await callback.message.edit_text(text, reply_markup=new_markup)
    
    await callback.answer()

# ========== ТОП КАНАЛОВ ПО РОСТУ ==========
@dp.callback_query(F.data == "top_growth")
async def top_growth_handler(callback: CallbackQuery):
    """Выбор периода для топа по росту"""
    text = "📈 Выберите период для топа каналов по росту:"
    if callback.message.text != text:
        await callback.message.edit_text(text, reply_markup=get_growth_menu())
    await callback.answer()

@dp.callback_query(F.data.startswith("growth_"))
async def growth_period_handler(callback: CallbackQuery):
    """Топ каналов по росту за период (15 позиций)"""
    period = callback.data.replace("growth_", "")
    period_text = "7 дней" if period == "7d" else "30 дней"
    
    channels = await db.get_top_channels_by_growth(period, 15)
    
    if not channels:
        text = f"📭 Пока нет данных о росте каналов за {period_text}.\n\nДобавьте каналы и подождите обновления."
        if callback.message.text != text:
            await callback.message.edit_text(text, reply_markup=get_main_menu())
        await callback.answer()
        return
    
    text = f"🚀 Топ-15 каналов по росту (за {period_text}):\n\n"
    kb = InlineKeyboardBuilder()
    
    for idx, (channel_id, username, title, subscribers, growth_7d, growth_30d) in enumerate(channels, 1):
        growth = growth_7d if period == "7d" else growth_30d
        
        text += f"{idx}. {title}\n"
        text += f"   📈 {growth:+.1f}% | 👥 {subscribers:,} подписчиков\n"
        
        btn_text = f"#{idx} {title[:15]}"
        if len(title) > 15:
            btn_text += "..."
        
        kb.button(text=btn_text, callback_data=f"channel_{channel_id}")
    
    kb.button(text="📅 Выбрать период", callback_data="top_growth")
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1)
    
    new_markup = kb.as_markup()
    if callback.message.text != text or callback.message.reply_markup != new_markup:
        await callback.message.edit_text(text, reply_markup=new_markup)
    
    await callback.answer()

# ========== ТОП МАЛЫЕ КАНАЛЫ (<3000) ==========
@dp.callback_query(F.data == "top_small")
async def top_small_channels_handler(callback: CallbackQuery):
    """Топ постов для каналов с менее 3000 подписчиков"""
    posts = await db.get_top_posts_small_channels(15)
    
    if not posts:
        text = "📭 Пока нет данных о малых каналах (<3000 подписчиков).\n\nДобавленные каналы обновляются каждые 30 минут."
        if callback.message.text != text:
            await callback.message.edit_text(text, reply_markup=get_main_menu())
        await callback.answer()
        return
    
    now = datetime.now()
    weekdays = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    weekday = weekdays[now.weekday()]
    date_str = now.strftime('%d %B')
    
    text = f"""📊 ТОП 15: наиболее читаемые посты каналов Каталога
(для каналов с аудиторией менее 3000 подписчиков).
{weekday}, {date_str}

"""
    
    kb = InlineKeyboardBuilder()
    
    for idx, (channel_id, username, title, message_id, views, post_date, post_text) in enumerate(posts, 1):
        views_formatted = format_number(views)
        clean_username = username[1:] if username.startswith('@') else username
        post_link = f"https://t.me/{clean_username}/{message_id}"
        preview = get_title_from_text(post_text, 7)
        
        text += f"{idx}. {title} ({post_link}): «{preview}» — {views_formatted};\n\n"
        
        btn_text = f"#{idx} {title[:15]}"
        if len(title) > 15:
            btn_text += "..."
        
        kb.button(text=btn_text, callback_data=f"post_{channel_id}_{message_id}")
    
    text += "\nНе важно сколько у вас подписчиков. Важно – сколько с интересом читают."
    
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1)
    
    new_markup = kb.as_markup()
    if callback.message.text != text or callback.message.reply_markup != new_markup:
        await callback.message.edit_text(text, reply_markup=new_markup)
    
    await callback.answer()

# ========== ПРОСМОТР ПОСТА ==========
@dp.callback_query(F.data.startswith("post_"))
async def show_post_handler(callback: CallbackQuery):
    """Показать пост с первыми 10 словами"""
    _, channel_id, message_id = callback.data.split("_")
    channel_id = int(channel_id)
    message_id = int(message_id)
    
    channel = await db.get_channel(channel_id)
    if not channel:
        await callback.answer("❌ Канал не найден")
        return
    
    username = channel[1]
    title = channel[2]
    post_text = await db.get_post_text(channel_id, message_id)
    preview_text = get_title_from_text(post_text, 10)
    
    clean_username = username[1:] if username.startswith('@') else username
    link = f"https://t.me/{clean_username}/{message_id}"
    
    await callback.message.answer(
        f"📢 Пост из канала {title}\n\n"
        f"📝 Смысл поста: {preview_text}\n\n"
        f"🔗 Ссылка на пост: {link}",
        reply_markup=InlineKeyboardBuilder()
            .button(text="🔗 Открыть пост", url=link)
            .button(text="🏠 В меню", callback_data="main_menu")
            .adjust(1)
            .as_markup()
    )
    await callback.answer()

# ========== ПРОСМОТР КАНАЛА ==========
@dp.callback_query(F.data.startswith("channel_"))
async def show_channel_handler(callback: CallbackQuery):
    """Показать информацию о канале"""
    channel_id = int(callback.data.split("_")[1])
    channel = await db.get_channel(channel_id)
    
    if not channel:
        await callback.answer("❌ Канал не найден")
        return
    
    channel_id, username, title, description, added_by, status, subscribers, growth_7d, growth_30d, created_at, updated_at = channel
    
    text = f"""📢 {title}

{description or 'Христианский канал'}

📊 Реальная статистика:
• Подписчики: {subscribers:,}
• Рост за 7 дней: {growth_7d:+.1f}%
• Рост за 30 дней: {growth_30d:+.1f}%
• Обновлено: {updated_at.strftime('%Y-%m-%d %H:%M') if updated_at else 'сегодня'}"""
    
    clean_username = username[1:] if username.startswith('@') else username
    link = f"https://t.me/{clean_username}"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Перейти в канал", url=link)
    kb.button(text="📊 Лучшие посты", callback_data=f"channel_posts_{channel_id}")
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1)
    
    await callback.message.answer(text, reply_markup=kb.as_markup())
    await callback.answer()

# ========== О ПРОЕКТЕ ==========
@dp.callback_query(F.data == "about")
async def about_handler(callback: CallbackQuery):
    """Информация о проекте"""
    text = """📖 Christian Channels Catalog

Каталог христианских Telegram-каналов с реальной статистикой.

🤖 Как это работает:
1. Пользователи добавляют каналы
2. Бот собирает реальную статистику: подписчики, посты, реакции
3. Каналы появляются в топах на основе реальных данных

📊 Доступные рейтинги (ТОП-15):
• Топ посты по реакциям - самые обсуждаемые посты
• Топ посты по просмотрам - самые популярные посты
• Топ посты по репостам - самые расшариваемые посты
• Топ каналы по росту - быстрорастущие сообщества
• Топ малые каналы - для каналов <3000 подписчиков

➕ Добавляйте свои каналы и находите единомышленников!"""
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 В меню", callback_data="main_menu")
    
    if callback.message.text != text or callback.message.reply_markup != kb.as_markup():
        await callback.message.edit_text(text, reply_markup=kb.as_markup())
    
    await callback.answer()

# ========== ДОБАВЛЕНИЕ КАНАЛА ==========
@dp.callback_query(F.data == "add_channel")
async def add_channel_start(callback: CallbackQuery, state: FSMContext):
    """Начало добавления канала - ЛЮБОЙ ПОЛЬЗОВАТЕЛЬ"""
    user_id = callback.from_user.id
    count = await db.get_user_channels_count(user_id)
    
    if count >= 5:
        text = "❌ Вы уже добавили 5 каналов (максимум)."
        if callback.message.text != text:
            await callback.message.edit_text(text, reply_markup=get_back_menu())
        await callback.answer()
        return
    
    text = """➕ Добавление канала

✅ ЛЮБОЙ ПОЛЬЗОВАТЕЛЬ МОЖЕТ ДОБАВИТЬ КАНАЛ:

1. Добавьте @christian_catalog_bot в администраторы вашего канала
2. Пришлите ссылку на канал:
   • @username
   • t.me/username
   • https://t.me/username

⚠️ ВАЖНО: Бот должен быть администратором канала для сбора статистики!

Канал появится в каталоге после одобрения модератором.

❌ Чтобы отменить добавление, нажми кнопку "Отмена" ниже."""
    
    kb = get_cancel_menu()
    
    if callback.message.text != text or callback.message.reply_markup != kb:
        await callback.message.edit_text(text, reply_markup=kb)
    
    await state.set_state(ChannelStates.waiting_link)
    await callback.answer()

@dp.message(ChannelStates.waiting_link)
async def process_channel_link(message: Message, state: FSMContext):
    """Обработка ссылки на канал - БЕЗ ПРОВЕРКИ"""
    link = message.text.strip()
    
    if not (link.startswith('@') or 't.me/' in link):
        await message.answer(
            "❌ Неверный формат. Нужно: @username или t.me/username\n\nОтправь ссылку еще раз или нажми /start для отмены."
        )
        return
    
    if link.startswith('@'):
        username = link
    else:
        parts = link.split('t.me/')[-1].split('/')
        username = '@' + parts[0]
    
    existing = await db.get_channel_by_username(username)
    if existing:
        status = existing[5]
        if status == 'pending':
            await message.answer(
                f"⏳ Канал {username} уже отправлен на модерацию.",
                reply_markup=get_main_menu()
            )
        elif status == 'approved':
            await message.answer(
                f"✅ Канал {username} уже одобрен и есть в каталоге.",
                reply_markup=get_main_menu()
            )
        else:
            await message.answer(
                f"❌ Канал {username} был отклонен.",
                reply_markup=get_main_menu()
            )
        await state.clear()
        return
    
    title = f"Канал {username}"
    if await db.add_channel(username, title, message.from_user.id):
        await message.answer(
            f"✅ Заявка на канал {username} отправлена на модерацию!\n\n"
            f"Администратор проверит заявку в течение 24 часов.",
            reply_markup=get_main_menu()
        )
        
        for admin_id in config.ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"📢 Новая заявка на добавление канала!\n\n"
                    f"📌 Канал: {username}\n"
                    f"👤 Добавил: @{message.from_user.username or 'нет юзернейма'} (ID: {message.from_user.id})\n"
                    f"📅 Время: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
                    f"🔍 Проверьте заявку в админ-панели: /admin"
                )
            except:
                pass
    else:
        await message.answer(
            "❌ Ошибка при добавлении канала в базу данных.",
            reply_markup=get_main_menu()
        )
    
    await state.clear()

# ========== ФУНКЦИИ ДЛЯ ОТЧЕТОВ (ТОП-15) ==========
async def generate_reactions_report():
    """Топ-15 постов по реакциям - ЕЖЕНЕДЕЛЬНЫЙ"""
    try:
        posts = await db.get_top_posts_by_reactions(15)
        if not posts:
            return None
        
        vladivostok_tz = pytz.timezone('Asia/Vladivostok')
        now = datetime.now(vladivostok_tz)
        weekdays = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        weekday = weekdays[now.weekday()]
        date_str = now.strftime('%d %B %Y')
        
        text = f"📊 ЕЖЕНЕДЕЛЬНЫЙ ОТЧЕТ: Топ-15 постов по реакциям\n"
        text += f"{weekday}, {date_str}\n\n"
        
        for idx, (channel_id, username, title, message_id, reactions, post_date, post_text) in enumerate(posts, 1):
            clean_username = username[1:] if username.startswith('@') else username
            channel_link = f"https://t.me/{clean_username}"
            post_link = f"https://t.me/{clean_username}/{message_id}"
            post_preview = get_title_from_text(post_text, 15)
            
            # ИСПРАВЛЕНО: Используем title (название канала) вместо username
            text += f"{idx}. [{title}]({channel_link}) | ❤️ {reactions} | [ПОСТ]({post_link})\n"
            text += f"   📝 {post_preview}\n\n"
        
        return text
        
    except Exception as e:
        print(f"Ошибка генерации отчета по реакциям: {e}")
        return None

async def generate_views_report():
    """Топ-15 постов по просмотрам - ЕЖЕНЕДЕЛЬНЫЙ"""
    try:
        posts = await db.get_top_posts_by_views(15)
        if not posts:
            return None
        
        vladivostok_tz = pytz.timezone('Asia/Vladivostok')
        now = datetime.now(vladivostok_tz)
        weekdays = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        weekday = weekdays[now.weekday()]
        date_str = now.strftime('%d %B %Y')
        
        text = f"📊 ЕЖЕНЕДЕЛЬНЫЙ ОТЧЕТ: Топ-15 постов по просмотрам\n"
        text += f"{weekday}, {date_str}\n\n"
        
        for idx, (channel_id, username, title, message_id, views, post_date, post_text) in enumerate(posts, 1):
            clean_username = username[1:] if username.startswith('@') else username
            channel_link = f"https://t.me/{clean_username}"
            post_link = f"https://t.me/{clean_username}/{message_id}"
            views_formatted = format_number(views)
            post_preview = get_title_from_text(post_text, 15)
            
            # ИСПРАВЛЕНО: Используем title (название канала) вместо username
            text += f"{idx}. [{title}]({channel_link}) | 👁️ {views_formatted} | [ПОСТ]({post_link})\n"
            text += f"   📝 {post_preview}\n\n"
        
        return text
        
    except Exception as e:
        print(f"Ошибка генерации отчета по просмотрам: {e}")
        return None

async def generate_forwards_report():
    """Топ-15 постов по репостам - ЕЖЕНЕДЕЛЬНЫЙ"""
    try:
        posts = await db.get_top_posts_by_forwards(15)
        if not posts:
            return None
        
        vladivostok_tz = pytz.timezone('Asia/Vladivostok')
        now = datetime.now(vladivostok_tz)
        weekdays = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        weekday = weekdays[now.weekday()]
        date_str = now.strftime('%d %B %Y')
        
        text = f"📊 ЕЖЕНЕДЕЛЬНЫЙ ОТЧЕТ: Топ-15 постов по репостам\n"
        text += f"{weekday}, {date_str}\n\n"
        
        for idx, (channel_id, username, title, message_id, forwards, post_date, post_text) in enumerate(posts, 1):
            clean_username = username[1:] if username.startswith('@') else username
            channel_link = f"https://t.me/{clean_username}"
            post_link = f"https://t.me/{clean_username}/{message_id}"
            post_preview = get_title_from_text(post_text, 15)
            
            # ИСПРАВЛЕНО: Используем title (название канала) вместо username
            text += f"{idx}. [{title}]({channel_link}) | 🔄 {forwards} | [ПОСТ]({post_link})\n"
            text += f"   📝 {post_preview}\n\n"
        
        return text
        
    except Exception as e:
        print(f"Ошибка генерации отчета по репостам: {e}")
        return None

async def generate_growth_report():
    """Топ-15 каналов по росту - ЕЖЕМЕСЯЧНЫЙ"""
    try:
        channels = await db.get_top_channels_by_growth('30d', 15)
        if not channels:
            return None
        
        vladivostok_tz = pytz.timezone('Asia/Vladivostok')
        now = datetime.now(vladivostok_tz)
        weekdays = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        weekday = weekdays[now.weekday()]
        date_str = now.strftime('%d %B %Y')
        
        text = f"📊 ЕЖЕМЕСЯЧНЫЙ ОТЧЕТ: Топ-15 каналов по росту (за 30 дней)\n"
        text += f"{weekday}, {date_str}\n\n"
        
        for idx, (channel_id, username, title, subscribers, growth_7d, growth_30d) in enumerate(channels, 1):
            clean_username = username[1:] if username.startswith('@') else username
            channel_link = f"https://t.me/{clean_username}"
            channel_with_link = f"[{title}]({channel_link})"
            
            text += f"{idx}. {channel_with_link}\n"
            text += f"   📈 {growth_30d:+.1f}% | 👥 {format_number(subscribers)} подписчиков\n\n"
        
        return text
        
    except Exception as e:
        print(f"Ошибка генерации отчета по росту: {e}")
        return None

async def generate_small_report():
    """Топ-15 постов малых каналов - ЕЖЕНЕДЕЛЬНЫЙ"""
    try:
        posts = await db.get_top_posts_small_channels(15)
        if not posts:
            return None
        
        vladivostok_tz = pytz.timezone('Asia/Vladivostok')
        now = datetime.now(vladivostok_tz)
        weekdays = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        weekday = weekdays[now.weekday()]
        date_str = now.strftime('%d %B %Y')
        
        text = f"📊 ЕЖЕНЕДЕЛЬНЫЙ ОТЧЕТ: Топ-15 постов малых каналов (<3000 подписчиков)\n"
        text += f"{weekday}, {date_str}\n\n"
        
        for idx, (channel_id, username, title, message_id, views, post_date, post_text) in enumerate(posts, 1):
            clean_username = username[1:] if username.startswith('@') else username
            channel_link = f"https://t.me/{clean_username}"
            post_link = f"https://t.me/{clean_username}/{message_id}"
            views_formatted = format_number(views)
            post_preview = get_title_from_text(post_text, 15)
            
            # ИСПРАВЛЕНО: Используем title (название канала) вместо username
            text += f"{idx}. [{title}]({channel_link}) | 👁️ {views_formatted} | [ПОСТ]({post_link})\n"
            text += f"   📝 {post_preview}\n\n"
        
        return text
        
    except Exception as e:
        print(f"Ошибка генерации отчета по малым каналам: {e}")
        return None

async def send_weekly_reports():
    """Отправка всех отчетов"""
    try:
        if not REPORT_CHANNEL_ID:
            print("⚠️ ID канала для отчетов не указан")
            return
        
        reports = [
            ("реакциям", await generate_reactions_report()),
            ("просмотрам", await generate_views_report()),
            ("репостам", await generate_forwards_report()),
            ("росту", await generate_growth_report()),
            ("малым каналам", await generate_small_report())
        ]
        
        sent_count = 0
        for name, report in reports:
            if report:
                try:
                    await bot.send_message(REPORT_CHANNEL_ID, report, parse_mode=ParseMode.MARKDOWN)
                    await asyncio.sleep(2)
                    sent_count += 1
                    print(f"✅ Отчет по {name} отправлен")
                except Exception as e:
                    print(f"❌ Ошибка отправки отчета по {name}: {e}")
                    try:
                        clean_report = report.replace('[', '').replace('](', ' - ').replace(')', '')
                        await bot.send_message(REPORT_CHANNEL_ID, clean_report)
                        print(f"✅ Отчет по {name} отправлен без форматирования")
                    except:
                        pass
        
        print(f"✅ Всего отправлено отчетов: {sent_count}")
        
    except Exception as e:
        print(f"❌ Ошибка отправки отчетов: {e}")

async def schedule_weekly_reports():
    """Планировщик еженедельных отчетов"""
    try:
        while True:
            vladivostok_tz = pytz.timezone('Asia/Vladivostok')
            now = datetime.now(vladivostok_tz)
            
            if now.weekday() == 5 and now.hour == 7 and now.minute == 0:
                print("📅 Суббота 7:00 - отправляю отчеты")
                await send_weekly_reports()
                await asyncio.sleep(3600)
            else:
                await asyncio.sleep(30)
                
    except Exception as e:
        print(f"❌ Ошибка планировщика: {e}")

# ========== АДМИН ПАНЕЛЬ ==========
@dp.message(Command("admin"))
async def admin_handler(message: Message):
    """Админ-панель"""
    if message.from_user.id not in config.ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к админ-панели")
        return
    
    pending = await db.get_pending_channels()
    all_channels = await db.get_all_channels()
    
    approved_count = len([c for c in all_channels if c[3] == 'approved'])
    pending_count = len(pending)
    total_count = len(all_channels)
    
    text = f"""⚙️ Панель администратора

📊 Статистика:
• Всего каналов: {total_count}
• Одобрено: {approved_count}
• На модерации: {pending_count}
• Отклонено: {total_count - approved_count - pending_count}

⚡ Выберите действие:"""
    
    kb = InlineKeyboardBuilder()
    
    if pending:
        kb.button(text=f"📋 Заявки ({pending_count})", callback_data="admin_pending")
    
    kb.button(text="📊 Все каналы", callback_data="admin_all_channels")
    kb.button(text="🔄 Обновить статистику", callback_data="admin_update_stats")
    kb.button(text="📅 Отправить тестовые отчеты", callback_data="admin_test_reports")
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1)
    
    await message.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "admin_test_reports")
async def admin_test_reports_handler(callback: CallbackQuery):
    """Тестовые отчеты"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("❌ Нет прав")
        return
    
    await callback.answer("🔄 Генерирую тестовые отчеты...", show_alert=False)
    await send_weekly_reports()
    await callback.answer("✅ Тестовые отчеты отправлены!", show_alert=True)

@dp.callback_query(F.data == "admin_pending")
async def admin_pending_handler(callback: CallbackQuery):
    """Заявки на модерацию"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("❌ Нет прав")
        return
    
    pending = await db.get_pending_channels()
    
    if not pending:
        text = "📭 Нет заявок на модерацию."
        kb = InlineKeyboardBuilder()
        kb.button(text="⚙️ В админку", callback_data="admin_back")
        kb.button(text="🏠 В меню", callback_data="main_menu")
        
        new_markup = kb.adjust(1).as_markup()
        if callback.message.text != text or callback.message.reply_markup != new_markup:
            await callback.message.edit_text(text, reply_markup=new_markup)
        
        await callback.answer()
        return
    
    text = "📋 Заявки на модерацию:\n\n"
    kb = InlineKeyboardBuilder()
    
    for channel_id, username, title, added_by, created_at in pending:
        # ИСПРАВЛЕНО: created_at - это datetime объект
        if created_at:
            if isinstance(created_at, datetime):
                date_str = created_at.strftime('%Y-%m-%d')
            else:
                date_str = str(created_at)[:10]
        else:
            date_str = "давно"
        
        text += f"• {title}\n  👤 {username}\n  📅 {date_str}\n  ID: {channel_id}\n\n"
        
        short_title = title[:10] + "..." if len(title) > 10 else title
        kb.button(text=f"✅ Одобрить {short_title}", callback_data=f"approve_{channel_id}")
        kb.button(text=f"❌ Отклонить {short_title}", callback_data=f"reject_{channel_id}")
    
    kb.button(text="🔄 Обновить", callback_data="admin_pending")
    kb.button(text="⚙️ В админку", callback_data="admin_back")
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1, 1)
    
    new_markup = kb.as_markup()
    if callback.message.text != text or callback.message.reply_markup != new_markup:
        await callback.message.edit_text(text, reply_markup=new_markup)
    
    await callback.answer()

@dp.callback_query(F.data.startswith("approve_"))
async def approve_channel_handler(callback: CallbackQuery):
    """Одобрить канал"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("❌ Нет прав")
        return
    
    channel_id = int(callback.data.replace("approve_", ""))
    
    if await db.approve_channel(channel_id):
        channel = await db.get_channel(channel_id)
        if channel:
            username = channel[1]
            try:
                await telegram_parser.connect()
                await telegram_parser.update_channel_stats(username, db)
            except Exception as e:
                print(f"⚠️ Не удалось собрать статистику: {e}")
        
        await callback.answer("✅ Канал одобрен!", show_alert=True)
        await admin_pending_handler(callback)
    else:
        await callback.answer("❌ Ошибка одобрения", show_alert=True)

@dp.callback_query(F.data.startswith("reject_"))
async def reject_channel_handler(callback: CallbackQuery):
    """Отклонить канал"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("❌ Нет прав")
        return
    
    channel_id = int(callback.data.replace("reject_", ""))
    
    if await db.reject_channel(channel_id):
        await callback.answer("✅ Канал отклонен!", show_alert=True)
        await admin_pending_handler(callback)
    else:
        await callback.answer("❌ Ошибка отклонения", show_alert=True)

@dp.callback_query(F.data == "admin_all_channels")
async def admin_all_channels_handler(callback: CallbackQuery):
    """Все каналы для админа"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("❌ Нет прав")
        return
    
    channels = await db.get_all_channels()
    
    if not channels:
        text = "📭 В базе нет каналов."
        kb = InlineKeyboardBuilder()
        kb.button(text="⚙️ В админку", callback_data="admin_back")
        kb.button(text="🏠 В меню", callback_data="main_menu")
        
        new_markup = kb.adjust(1).as_markup()
        if callback.message.text != text or callback.message.reply_markup != new_markup:
            await callback.message.edit_text(text, reply_markup=new_markup)
        
        await callback.answer()
        return
    
    text = "📋 Все каналы в базе:\n\n"
    kb = InlineKeyboardBuilder()
    
    for channel_id, username, title, status, subscribers in channels:
        status_icon = "✅" if status == 'approved' else "⏳" if status == 'pending' else "❌"
        text += f"{status_icon} {title}\n"
        text += f"   👤 {username} | 👥 {subscribers:,} | ID: {channel_id}\n\n"
        
        if status == 'approved':
            short_title = title[:15] + "..." if len(title) > 15 else title
            kb.button(text=f"🗑️ Удалить {short_title}", callback_data=f"delete_{channel_id}")
        elif status == 'pending':
            short_title = title[:10] + "..." if len(title) > 10 else title
            kb.button(text=f"✅ Одобрить {short_title}", callback_data=f"approve_{channel_id}")
            kb.button(text=f"❌ Отклонить {short_title}", callback_data=f"reject_{channel_id}")
    
    kb.button(text="🔄 Обновить", callback_data="admin_all_channels")
    kb.button(text="⚙️ В админку", callback_data="admin_back")
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1)
    
    new_markup = kb.as_markup()
    if callback.message.text != text or callback.message.reply_markup != new_markup:
        await callback.message.edit_text(text, reply_markup=new_markup)
    else:
        await callback.answer("✅ Данные актуальны")
    
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_"))
async def delete_channel_handler(callback: CallbackQuery):
    """Удалить канал"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("❌ Нет прав")
        return
    
    channel_id = int(callback.data.replace("delete_", ""))
    
    if await db.delete_channel(channel_id):
        await callback.answer("✅ Канал удален!", show_alert=True)
        await admin_all_channels_handler(callback)
    else:
        await callback.answer("❌ Ошибка удаления", show_alert=True)

@dp.callback_query(F.data == "admin_update_stats")
async def admin_update_stats_handler(callback: CallbackQuery):
    """Обновить статистику всех каналов"""
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("❌ Нет прав")
        return
    
    await callback.answer("🔄 Начинаю обновление...", show_alert=False)
    
    try:
        await telegram_parser.connect()
        results = await telegram_parser.update_all_channels(db)
        
        text = f"✅ Обновлено {len(results)} каналов\n\n"
        if results:
            text += "Последние обновления:\n"
            for result in results[:5]:
                text += f"• {result['title']}: {result['subscribers']:,} подписчиков\n"
        
        await callback.message.answer(text, reply_markup=get_main_menu())
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {str(e)}", reply_markup=get_main_menu())

@dp.callback_query(F.data == "admin_back")
async def admin_back_handler(callback: CallbackQuery, state: FSMContext):
    """Назад в админ-панель"""
    await state.clear()
    
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("❌ Нет прав")
        return
    
    pending = await db.get_pending_channels()
    all_channels = await db.get_all_channels()
    
    approved_count = len([c for c in all_channels if c[3] == 'approved'])
    pending_count = len(pending)
    total_count = len(all_channels)
    
    text = f"""⚙️ Панель администратора

📊 Статистика:
• Всего каналов: {total_count}
• Одобрено: {approved_count}
• На модерации: {pending_count}
• Отклонено: {total_count - approved_count - pending_count}

⚡ Выберите действие:"""
    
    kb = InlineKeyboardBuilder()
    
    if pending:
        kb.button(text=f"📋 Заявки ({pending_count})", callback_data="admin_pending")
    
    kb.button(text="📊 Все каналы", callback_data="admin_all_channels")
    kb.button(text="🔄 Обновить статистику", callback_data="admin_update_stats")
    kb.button(text="📅 Отправить тестовые отчеты", callback_data="admin_test_reports")
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1)
    
    new_markup = kb.as_markup()
    if callback.message.text != text or callback.message.reply_markup != new_markup:
        await callback.message.edit_text(text, reply_markup=new_markup)
    else:
        await callback.message.answer(text, reply_markup=new_markup)
    
    await callback.answer()

# ========== АВТООБНОВЛЕНИЕ ==========
async def scheduled_parser():
    """Автообновление статистики"""
    try:
        if await telegram_parser.connect():
            print(f"\n⏰ {datetime.now().strftime('%H:%M')} - Автообновление...")
            results = await telegram_parser.update_all_channels(db)
            print(f"✅ Обновлено {len(results)} каналов")
    except Exception as e:
        print(f"❌ Ошибка автообновления: {e}")

# ========== ЗАПУСК ==========
async def main():
    print("\n" + "="*60)
    print("🤖 КАТАЛОГ ХРИСТИАНСКИХ КАНАЛОВ")
    print("="*60)
    
    # Подключаемся к PostgreSQL
    try:
        await db.connect()
        print("✅ База данных: PostgreSQL")
    except Exception as e:
        print(f"❌ Критическая ошибка: не удалось подключиться к БД")
        print(f"❌ {e}")
        return
    
    print(f"👑 Админы: {config.ADMIN_IDS}")
    print(f"🔧 API_ID: {config.API_ID}")
    print(f"📊 Топы: 15 позиций")
    print(f"📅 Отчеты: Суббота 7:00 (Владивосток)")
    print("="*60)
    
    print("\n🔗 Тестирую подключение парсера...")
    try:
        if await telegram_parser.connect():
            print("✅ Парсер подключен!")
        else:
            print("⚠️ Парсер не подключен")
    except Exception as e:
        print(f"❌ Ошибка парсера: {e}")
    
    print("\n🚀 Запускаю бота...")
    print("✅ Бот запущен!")
    print("="*60)
    
    async def background_parser():
        while True:
            await scheduled_parser()
            await asyncio.sleep(config.PARSE_INTERVAL)
    
    async def background_reports():
        await schedule_weekly_reports()
    
    asyncio.create_task(background_parser())
    asyncio.create_task(background_reports())
    
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    except Exception as e:
        print(f"❌ Ошибка запуска бота: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Бот остановлен")
        asyncio.run(telegram_parser.close())



