import os
from dotenv import load_dotenv

load_dotenv()

# Telegram Bot
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

# Telegram API для парсера
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH")

# Канал для еженедельных отчетов
REPORT_CHANNEL_ID = os.getenv("REPORT_CHANNEL_ID")

# Настройки
PARSE_INTERVAL = 1800  # 30 минут
POSTS_LIMIT = 30  # Постов для парсинга
