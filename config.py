import os
from dotenv import load_dotenv

load_dotenv()

# Telegram Bot
BOT_TOKEN = os.getenv("BOT_TOKEN")
# Поддерживаем несколько ID через запятую
ADMIN_IDS = []
admin_ids_str = os.getenv("ADMIN_ID", "0")
for id_str in admin_ids_str.split(","):
    if id_str.strip():
        try:
            ADMIN_IDS.append(int(id_str.strip()))
        except ValueError:
            pass

# Для обратной совместимости оставляем ADMIN_ID как первый ID
ADMIN_ID = ADMIN_IDS[0] if ADMIN_IDS else 0

# Telegram API для парсера
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH")

# Канал для еженедельных отчетов
REPORT_CHANNEL_ID = os.getenv("REPORT_CHANNEL_ID")

# Настройки
PARSE_INTERVAL = 1800  # 30 минут
# POSTS_LIMIT больше не используется, удаляем
