import asyncio
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder

import config
import database
import parser
import pytz 

# ========== ИНИЦИАЛИЗАЦИЯ ==========
bot = Bot(token=config.BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

db = database.Database()
telegram_parser = parser.TelegramParser()

# ID канала для отчетов
REPORT_CHANNEL_ID = config.REPORT_CHANNEL_ID

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ОТЧЕТОВ ==========
def smart_title(text: str, max_words: int = 15, max_chars: int = 100) -> str:
    """
    Умное формирование заголовка из текста поста
    - Берем первые max_words слов
    - Обрезаем по символам если нужно
    - Убираем лишние пробелы и спецсимволы
    """
    if not text:
        return "Без текста"
    
    # Очищаем текст от лишних пробелов и переносов строк
    clean_text = ' '.join(text.split())
    
    # Разбиваем на слова
    words = clean_text.split()[:max_words]
    
    if not words:
        return "Без текста"
    
    # Собираем заголовок
    title = ' '.join(words)
    
    # Если получилось слишком длинно - обрезаем по символам
    if len(title) > max_chars:
        title = title[:max_chars-3] + "..."
    elif len(clean_text.split()) > max_words:
        title += "..."
    
    return title

def format_number(num: int) -> str:
    """Форматирование чисел (1000 -> 1K)"""
    if num >= 1000000:
        return f"{num/1000000:.1f}M".replace('.0M', 'M')
    if num >= 1000:
        return f"{num/1000:.1f}K".replace('.0K', 'K')
    return str(num)

# ========== ОСТАВЛЯЕМ ВЕСЬ ТВОЙ СУЩЕСТВУЮЩИЙ КОД БЕЗ ИЗМЕНЕНИЙ ==========
# (States, клавиатуры, обработчики команд - всё как было)
# Вставляй сюда весь свой существующий код main.py
# Я покажу только новые функции отчетов, которые нужно ЗАМЕНИТЬ

# ========== НОВЫЕ ФУНКЦИИ ДЛЯ ЕЖЕНЕДЕЛЬНЫХ ОТЧЕТОВ (ЗАМЕНИ ПОЛНОСТЬЮ) ==========

async def generate_weekly_report():
    """Генерация еженедельного отчета в HTML-формате"""
    try:
        vladivostok_tz = pytz.timezone('Asia/Vladivostok')
        now = datetime.now(vladivostok_tz)
        weekdays = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        weekday = weekdays[now.weekday()]
        date_str = now.strftime('%d %B %Y')
        
        # ========== 1. ЕЖЕНЕДЕЛЬНЫЙ ОТЧЕТ: ТОП ПО РЕАКЦИЯМ ==========
        reactions_posts = db.get_top_posts_by_reactions(100)  # Увеличил до 100
        report_reactions = f"""📊 <b>ЕЖЕНЕДЕЛЬНЫЙ ОТЧЕТ: Топ-100 постов по реакциям</b>
{weekday}, {date_str}

"""
        for idx, (channel_id, username, title, message_id, reactions, post_date, post_text) in enumerate(reactions_posts, 1):
            clean_username = username[1:] if username.startswith('@') else username
            channel_link = f"https://t.me/{clean_username}"
            post_link = f"https://t.me/{clean_username}/{message_id}"
            
            # Умный заголовок из текста поста
            post_title = smart_title(post_text)
            
            report_reactions += f'{idx}. <a href="{channel_link}">{title}</a> | ❤️ {reactions} | <a href="{post_link}">ПОСТ</a>\n'
            report_reactions += f'   📝 <i>{post_title}</i>\n\n'
        
        # ========== 2. ЕЖЕНЕДЕЛЬНЫЙ ОТЧЕТ: ТОП ПО ПРОСМОТРАМ ==========
        views_posts = db.get_top_posts_by_views(100)  # Увеличил до 100
        report_views = f"""📊 <b>ЕЖЕНЕДЕЛЬНЫЙ ОТЧЕТ: Топ-100 постов по просмотрам</b>
{weekday}, {date_str}

"""
        for idx, (channel_id, username, title, message_id, views, post_date, post_text) in enumerate(views_posts, 1):
            clean_username = username[1:] if username.startswith('@') else username
            channel_link = f"https://t.me/{clean_username}"
            post_link = f"https://t.me/{clean_username}/{message_id}"
            
            views_formatted = format_number(views)
            
            # Умный заголовок из текста поста
            post_title = smart_title(post_text)
            
            report_views += f'{idx}. <a href="{channel_link}">{title}</a> | 👁️ {views_formatted} | <a href="{post_link}">ПОСТ</a>\n'
            report_views += f'   📝 <i>{post_title}</i>\n\n'
        
        # ========== 3. ЕЖЕМЕСЯЧНЫЙ ОТЧЕТ: ТОП ПО РЕПОСТАМ ==========
        forwards_posts = db.get_top_posts_by_forwards(100)  # Увеличил до 100
        report_forwards = f"""📊 <b>ЕЖЕМЕСЯЧНЫЙ ОТЧЕТ: Топ-100 постов по репостам</b>
{weekday}, {date_str}

"""
        for idx, (channel_id, username, title, message_id, forwards, post_date, post_text) in enumerate(forwards_posts, 1):
            clean_username = username[1:] if username.startswith('@') else username
            channel_link = f"https://t.me/{clean_username}"
            post_link = f"https://t.me/{clean_username}/{message_id}"
            
            # Умный заголовок из текста поста
            post_title = smart_title(post_text)
            
            report_forwards += f'{idx}. <a href="{channel_link}">{title}</a> | 🔄 {forwards} | <a href="{post_link}">ПОСТ</a>\n'
            report_forwards += f'   📝 <i>{post_title}</i>\n\n'
        
        # ========== 4. ЕЖЕМЕСЯЧНЫЙ ОТЧЕТ: ТОП КАНАЛОВ ПО РОСТУ ==========
        growth_channels = db.get_top_channels_by_growth('30d', 100)  # Увеличил до 100
        report_growth = f"""📊 <b>ЕЖЕМЕСЯЧНЫЙ ОТЧЕТ: Топ-100 каналов по росту (за 30 дней)</b>
{weekday}, {date_str}

"""
        for idx, (channel_id, username, title, subscribers, growth_7d, growth_30d) in enumerate(growth_channels, 1):
            clean_username = username[1:] if username.startswith('@') else username
            channel_link = f"https://t.me/{clean_username}"
            
            report_growth += f'{idx}. <a href="{channel_link}">{title}</a>\n'
            report_growth += f'   📈 {growth_30d:+.1f}% | 👥 {format_number(subscribers)} подписчиков\n\n'
        
        # ========== 5. ЕЖЕМЕСЯЧНЫЙ ОТЧЕТ: ТОП МАЛЫЕ КАНАЛЫ ==========
        small_posts = db.get_top_posts_small_channels(100)  # Увеличил до 100
        report_small = f"""📊 <b>ЕЖЕМЕСЯЧНЫЙ ОТЧЕТ: Топ-100 постов малых каналов (<3000 подписчиков)</b>
{weekday}, {date_str}

"""
        for idx, (channel_id, username, title, message_id, views, post_date, post_text) in enumerate(small_posts, 1):
            clean_username = username[1:] if username.startswith('@') else username
            channel_link = f"https://t.me/{clean_username}"
            post_link = f"https://t.me/{clean_username}/{message_id}"
            
            views_formatted = format_number(views)
            
            # Умный заголовок из текста поста
            post_title = smart_title(post_text)
            
            report_small += f'{idx}. <a href="{channel_link}">{title}</a> | 👁️ {views_formatted} | <a href="{post_link}">ПОСТ</a>\n'
            report_small += f'   📝 <i>{post_title}</i>\n\n'
        
        return {
            'reactions': report_reactions,
            'views': report_views,
            'forwards': report_forwards,
            'growth': report_growth,
            'small': report_small
        }
        
    except Exception as e:
        print(f"❌ Ошибка генерации отчета: {e}")
        return None

async def send_weekly_report():
    """Отправка еженедельного отчета в канал"""
    try:
        if not REPORT_CHANNEL_ID:
            print("⚠️ ID канала для отчетов не указан")
            return
        
        report = await generate_weekly_report()
        if not report:
            return
        
        # Отправляем 5 отчетов (2 еженедельных + 3 ежемесячных)
        await bot.send_message(REPORT_CHANNEL_ID, report['reactions'])
        await asyncio.sleep(1)
        await bot.send_message(REPORT_CHANNEL_ID, report['views'])
        await asyncio.sleep(1)
        await bot.send_message(REPORT_CHANNEL_ID, report['forwards'])
        await asyncio.sleep(1)
        await bot.send_message(REPORT_CHANNEL_ID, report['growth'])
        await asyncio.sleep(1)
        await bot.send_message(REPORT_CHANNEL_ID, report['small'])
        
        print(f"✅ Еженедельные отчеты отправлены в {REPORT_CHANNEL_ID}")
        
    except Exception as e:
        print(f"❌ Ошибка отправки отчета: {e}")

async def schedule_weekly_reports():
    """Планировщик еженедельных отчетов"""
    try:
        while True:
            vladivostok_tz = pytz.timezone('Asia/Vladivostok')
            now = datetime.now(vladivostok_tz)
            
            # Отправляем в субботу в 7:00 утра по Владивостоку
            if now.weekday() == 5 and now.hour == 7 and now.minute == 0:
                print("📅 Суббота 7:00 по Владивостоку - отправляю отчеты")
                await send_weekly_report()
                await asyncio.sleep(3600)  # Спим час, чтобы не отправить повторно
            else:
                await asyncio.sleep(30)  # Проверяем каждые 30 секунд
                
    except Exception as e:
        print(f"❌ Ошибка планировщика: {e}")

# ========== ВСЁ ОСТАЛЬНОЕ (States, клавиатуры, хендлеры) ОСТАВЛЯЕМ КАК ЕСТЬ ==========
# Твой существующий код main.py продолжается здесь...
# ========== АДМИН ПАНЕЛЬ ==========
@dp.message(Command("admin"))
async def admin_handler(message: Message):
    """Админ-панель"""
    if message.from_user.id != config.ADMIN_ID:
        await message.answer("❌ У вас нет доступа к админ-панели")
        return
    
    pending = db.get_pending_channels()
    all_channels = db.get_all_channels()
    
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
    kb.button(text="📅 Отправить тестовый отчет", callback_data="admin_test_report")
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1)
    
    await message.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "admin_test_report")
async def admin_test_report_handler(callback: CallbackQuery):
    """Тестовый отчет"""
    if callback.from_user.id != config.ADMIN_ID:
        await callback.answer("❌ Нет прав")
        return
    
    await callback.answer("🔄 Генерирую тестовый отчет...")
    await send_weekly_report()
    await callback.answer("✅ Тестовый отчет отправлен!", show_alert=True)

@dp.callback_query(F.data == "admin_pending")
async def admin_pending_handler(callback: CallbackQuery):
    """Заявки на модерацию"""
    if callback.from_user.id != config.ADMIN_ID:
        await callback.answer("❌ Нет прав")
        return
    
    pending = db.get_pending_channels()
    
    if not pending:
        await callback.message.edit_text(
            "📭 Нет заявок на модерацию.",
            reply_markup=InlineKeyboardBuilder()
                .button(text="⚙️ В админку", callback_data="admin_back")
                .button(text="🏠 В меню", callback_data="main_menu")
                .adjust(1)
                .as_markup()
        )
        await callback.answer()
        return
    
    text = "📋 Заявки на модерацию:\n\n"
    kb = InlineKeyboardBuilder()
    
    for channel_id, username, title, added_by, created_at in pending:
        date_str = created_at[:10] if created_at else "давно"
        text += f"• {title}\n  👤 {username}\n  📅 {date_str}\n  ID: {channel_id}\n\n"
        
        kb.button(text=f"✅ Одобрить {title[:10]}", callback_data=f"approve_{channel_id}")
        kb.button(text=f"❌ Отклонить {title[:10]}", callback_data=f"reject_{channel_id}")
    
    kb.button(text="🔄 Обновить", callback_data="admin_pending")
    kb.button(text="⚙️ В админку", callback_data="admin_back")
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1, 1)
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("approve_"))
async def approve_channel_handler(callback: CallbackQuery):
    """Одобрить канал"""
    if callback.from_user.id != config.ADMIN_ID:
        await callback.answer("❌ Нет прав")
        return
    
    channel_id = int(callback.data.replace("approve_", ""))
    
    if db.approve_channel(channel_id):
        channel = db.get_channel(channel_id)
        if channel:
            username = channel[1]
            try:
                await telegram_parser.connect()
                await telegram_parser.update_channel_stats(username, db)
            except Exception as e:
                print(f"⚠️ Не удалось собрать статистику: {e}")
        
        await callback.answer(f"✅ Канал одобрен!", show_alert=True)
        await admin_pending_handler(callback)
    else:
        await callback.answer("❌ Ошибка одобрения", show_alert=True)

@dp.callback_query(F.data.startswith("reject_"))
async def reject_channel_handler(callback: CallbackQuery):
    """Отклонить канал"""
    if callback.from_user.id != config.ADMIN_ID:
        await callback.answer("❌ Нет прав")
        return
    
    channel_id = int(callback.data.replace("reject_", ""))
    
    if db.reject_channel(channel_id):
        await callback.answer(f"✅ Канал отклонен!", show_alert=True)
        await admin_pending_handler(callback)
    else:
        await callback.answer("❌ Ошибка отклонения", show_alert=True)

@dp.callback_query(F.data == "admin_all_channels")
async def admin_all_channels_handler(callback: CallbackQuery):
    """Все каналы для админа"""
    if callback.from_user.id != config.ADMIN_ID:
        await callback.answer("❌ Нет прав")
        return
    
    channels = db.get_all_channels()
    
    if not channels:
        await callback.message.edit_text(
            "📭 В базе нет каналов.",
            reply_markup=InlineKeyboardBuilder()
                .button(text="⚙️ В админку", callback_data="admin_back")
                .button(text="🏠 В меню", callback_data="main_menu")
                .adjust(1)
                .as_markup()
        )
        await callback.answer()
        return
    
    text = "📋 Все каналы в базе:\n\n"
    kb = InlineKeyboardBuilder()
    
    for channel_id, username, title, status, subscribers in channels:
        status_icon = "✅" if status == 'approved' else "⏳" if status == 'pending' else "❌"
        text += f"{status_icon} {title}\n"
        text += f"   👤 {username} | 👥 {subscribers:,} | ID: {channel_id}\n\n"
        
        if status == 'approved':
            kb.button(text=f"🗑️ Удалить {title[:8]}", callback_data=f"delete_{channel_id}")
        elif status == 'pending':
            kb.button(text=f"✅ Одобрить {title[:8]}", callback_data=f"approve_{channel_id}")
            kb.button(text=f"❌ Отклонить {title[:8]}", callback_data=f"reject_{channel_id}")
    
    kb.button(text="🔄 Обновить", callback_data="admin_all_channels")
    kb.button(text="⚙️ В админку", callback_data="admin_back")
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_"))
async def delete_channel_handler(callback: CallbackQuery):
    """Удалить канал"""
    if callback.from_user.id != config.ADMIN_ID:
        await callback.answer("❌ Нет прав")
        return
    
    channel_id = int(callback.data.replace("delete_", ""))
    
    if db.delete_channel(channel_id):
        await callback.answer(f"✅ Канал удален!", show_alert=True)
        await admin_all_channels_handler(callback)
    else:
        await callback.answer("❌ Ошибка удаления", show_alert=True)

@dp.callback_query(F.data == "admin_update_stats")
async def admin_update_stats_handler(callback: CallbackQuery):
    """Обновить статистику всех каналов"""
    if callback.from_user.id != config.ADMIN_ID:
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
async def admin_back_handler(callback: CallbackQuery):
    """Назад в админ-панель"""
    if callback.from_user.id != config.ADMIN_ID:
        await callback.answer("❌ Нет прав")
        return
    
    pending = db.get_pending_channels()
    all_channels = db.get_all_channels()
    
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
    kb.button(text="📅 Отправить тестовый отчет", callback_data="admin_test_report")
    kb.button(text="🏠 В меню", callback_data="main_menu")
    kb.adjust(1)
    
    await callback.message.answer(text, reply_markup=kb.as_markup())
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
    print("🤖 CHRISTIAN CHANNELS CATALOG")
    print("="*60)
    print(f"👑 Админ: {config.ADMIN_ID}")
    print(f"🔧 API_ID: {config.API_ID}")
    print(f"📁 База: christian_catalog.db")
    print(f"📊 Топы: 20 позиций")
    print(f"📅 Отчеты: Суббота 7:00 (Владивосток)")
    print(f"👥 Режим: Любой пользователь может добавлять каналы")
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

