import os
from dotenv import load_dotenv

load_dotenv()

# Telegram Bot
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# Telegram API для парсера
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

# Прокси SOCKS5 (данные из вашего скриншота)
PROXY_HOST = "185.3.200.6"
PROXY_PORT = 2053
PROXY_SECRET = "636F7474692E79656B74616E65742E636F6D"  # Это ключ, не пароль!

# Канал для еженедельных отчетов
REPORT_CHANNEL_ID = os.getenv("REPORT_CHANNEL_ID", "@your_channel")

# Настройки
PARSE_INTERVAL = 1800  # 30 минут
POSTS_LIMIT = 30  # Постов для парсинга