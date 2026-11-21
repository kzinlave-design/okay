"""
WhatsApp-бот для автоматизации записи клиентов в баню.
Библиотека: neonize (вход по QR-коду, сессия хранится в SQLite).
Для Render: встроенный HTTP-сервер (health check и страница с QR-кодом /qr).

Машина состояний (по JID клиента):
    [нет состояния] --первое сообщение--> WAITING_FOR_DATA
    WAITING_FOR_DATA --данные заявки--> COMPLETED
    Любое состояние сбрасывается через SESSION_TIMEOUT секунд неактивности.
"""

import io
import logging
import os
import threading
import time
from dataclasses import dataclass
from enum import Enum, auto
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional

import qrcode
from neonize.client import NewClient
from neonize.events import ConnectedEv, MessageEv, QREv
from neonize.utils import build_jid


# ============================================================================
# НАСТРОЙКИ
# ============================================================================

# Файл сессии WhatsApp. На Render без постоянного диска сбрасывается при деплое.
SESSION_DB = os.environ.get("SESSION_DB", "db.sqlite3")

# Порт, который выдаёт Render (локально по умолчанию 10000)
PORT = int(os.environ.get("PORT", "10000"))

# Номер администратора: только цифры, с кодом страны, без «+».
# Переменная окружения ADMIN_PHONE, если задана в Render, имеет приоритет.
ADMIN_PHONE = os.environ.get("ADMIN_PHONE", "77787003325")

# Таймаут неактивности клиента, после которого состояние сбрасывается (секунды)
SESSION_TIMEOUT = 30 * 60
# Как часто фоновый поток очищает устаревшие состояния (секунды)
CLEANUP_INTERVAL = 60

# Автоответ на первое сообщение клиента
WELCOME_TEXT = (
    "Здравствуйте! Добро пожаловать в наш банный комплекс 🧖‍♀️🧖‍♂️\n\n"
    "Мы работаем без выходных:\n\n"
    "🕑 Понедельник — с 14:00 до 01:00\n"
    "🕛 Вторник–пятница — с 13:00 до 01:00\n"
    "🕚 Суббота–воскресенье — с 12:00 до 01:00\n\n"
    "✨ Женский и мужской залы\n"
    "✨ Время посещения — безлимит\n"
    "✨ Бесплатно выдаются тапочки\n"
    "✨ Массаж, пилинг и вкусная кухня ждут вас!\n\n"
    "👙 Для посещения бассейна и женского зала обязательно: купальник и шапочка\n\n"
    "Стоимость входа:\n"
    "▪️ Взрослый — 3 000 ₸\n"
    "▪️ Детский — 2 000 ₸\n"
    "▪️ Пенсионера — 2 500 ₸"
)

# Просьба прислать данные для брони (отправляется сразу после WELCOME_TEXT)
BOOKING_REQUEST_TEXT = (
    "Чтобы забронировать время, отправьте одним сообщением:\n"
    "1️⃣ Имя\n"
    "2️⃣ Желаемую дату и время\n"
    "3️⃣ Количество человек"
)

REQUEST_ACCEPTED_TEXT = (
    "Спасибо, заявка принята! Администратор скоро свяжется с вами для подтверждения брони."
)

NEED_TEXT_TEXT = (
    "Пожалуйста, отправьте данные для брони текстом: имя, дату и время, количество человек."
)

ADMIN_NOTIFICATION_TEMPLATE = (
    "🛎 Новая заявка на баню\n\n"
    "Клиент: https://wa.me/{phone}\n\n"
    "Текст заявки:\n{text}"
)


# ============================================================================
# ЛОГИРОВАНИЕ И КЛИЕНТ
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("bath_bot")

client = NewClient(SESSION_DB)

# Состояние для веб-страницы с QR-кодом
_latest_qr: Optional[str] = None
_connected = False


# ============================================================================
# МАШИНА СОСТОЯНИЙ
# ============================================================================

class State(Enum):
    WAITING_FOR_DATA = auto()
    COMPLETED = auto()


@dataclass
class UserSession:
    state: State
    last_active: float


_sessions: dict[str, UserSession] = {}
_sessions_lock = threading.Lock()


def _get_state(jid: str) -> Optional[State]:
    """Возвращает состояние клиента или None, если его нет или оно истекло."""
    with _sessions_lock:
        session = _sessions.get(jid)
        if session is None:
            return None
        if time.time() - session.last_active > SESSION_TIMEOUT:
            del _sessions[jid]
            log.info("Состояние %s истекло по таймауту", jid)
            return None
        return session.state


def _set_state(jid: str, state: State) -> None:
    with _sessions_lock:
        _sessions[jid] = UserSession(state=state, last_active=time.time())


def _touch(jid: str) -> None:
    """Обновляет время последней активности клиента."""
    with _sessions_lock:
        session = _sessions.get(jid)
        if session is not None:
            session.last_active = time.time()


def _cleanup_loop() -> None:
    """Фоновый поток: периодически удаляет устаревшие состояния из памяти."""
    while True:
        time.sleep(CLEANUP_INTERVAL)
        now = time.time()
        with _sessions_lock:
            expired = [
                jid for jid, s in _sessions.items()
                if now - s.last_active > SESSION_TIMEOUT
            ]
            for jid in expired:
                del _sessions[jid]
        if expired:
            log.info("Очищено устаревших состояний: %d", len(expired))


# ============================================================================
# ОТПРАВКА СООБЩЕНИЙ
# ============================================================================

def _send(jid, text: str) -> bool:
    """Безопасная отправка сообщения. Возвращает True при успехе."""
    try:
        client.send_message(jid, text)
        return True
    except Exception:
        log.exception("Ошибка отправки сообщения")
        return False


def _forward_to_admin(client_phone: str, text: str) -> bool:
    """Пересылает заявку администратору."""
    try:
        admin_jid = build_jid(ADMIN_PHONE)
        message = ADMIN_NOTIFICATION_TEMPLATE.format(phone=client_phone, text=text)
        client.send_message(admin_jid, message)
        return True
    except Exception:
        log.exception("Не удалось переслать заявку администратору")
        return False


# ============================================================================
# РАЗБОР СООБЩЕНИЯ
# ============================================================================

def _extract_text(message: MessageEv) -> str:
    """Достаёт текст из обычного или расширенного текстового сообщения."""
    msg = message.Message
    if msg.conversation:
        return msg.conversation.strip()
    if msg.extendedTextMessage and msg.extendedTextMessage.text:
        return msg.extendedTextMessage.text.strip()
    return ""


def _should_ignore(message: MessageEv) -> bool:
    """Игнорирует групповые сообщения, статусы и собственные исходящие сообщения."""
    src = message.Info.MessageSource

    if src.IsFromMe:
        return True

    chat = src.Chat
    if chat.Server == "g.us":  # группы
        return True
    if chat.User == "status" or chat.Server == "broadcast":  # статусы
        return True

    return False


# ============================================================================
# ОБРАБОТЧИКИ WHATSAPP
# ============================================================================

@client.event(QREv)
def on_qr(_: NewClient, event: QREv) -> None:
    global _latest_qr
    # Проверьте имя атрибута в документации вашей версии neonize
    _latest_qr = event.Codes[0]
    log.info("Получен новый QR-код, откройте /qr в браузере")


@client.event(ConnectedEv)
def on_connected(_: NewClient, __: ConnectedEv) -> None:
    global _latest_qr, _connected
    _connected = True
    _latest_qr = None
    log.info("Бот подключён к WhatsApp")


@client.event(MessageEv)
def on_message(_: NewClient, message: MessageEv) -> None:
    if _should_ignore(message):
        return

    src = message.Info.MessageSource
    chat = src.Chat
    # Ключ состояния и адрес ответа: JID клиента в личном чате
    jid_key = f"{chat.User}@{chat.Server}"
    client_phone = src.Sender.User

    text = _extract_text(message)
    state = _get_state(jid_key)

    try:
        # Первое сообщение клиента: автоответ и запрос данных для брони
        if state is None:
            _send(chat, WELCOME_TEXT)
            _send(chat, BOOKING_REQUEST_TEXT)
            _set_state(jid_key, State.WAITING_FOR_DATA)
            log.info("Новый клиент %s, ожидаем данные", client_phone)
            return

        # После отправки заявки бот молчит, чтобы не мешать администратору
        if state is State.COMPLETED:
            _touch(jid_key)
            return

        # Клиент прислал данные заявки
        if state is State.WAITING_FOR_DATA:
            if not text:
                _send(chat, NEED_TEXT_TEXT)
                _touch(jid_key)
                return

            _send(chat, REQUEST_ACCEPTED_TEXT)
            if _forward_to_admin(client_phone, text):
                log.info("Заявка от %s переслана администратору", client_phone)
            else:
                log.warning("Заявка от %s НЕ переслана, требуется проверка", client_phone)

            _set_state(jid_key, State.COMPLETED)

    except Exception:
        log.exception("Необработанная ошибка при обработке сообщения от %s", client_phone)


# ============================================================================
# ВЕБ-СЕРВЕР (health check и QR-код для Render)
# ============================================================================

class _WebHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.startswith("/qr"):
            if _connected or _latest_qr is None:
                self._reply(404, b"QR not available: bot is connected or code not generated yet",
                            "text/plain; charset=utf-8")
                return
            buf = io.BytesIO()
            qrcode.make(_latest_qr).save(buf, format="PNG")
            self._reply(200, buf.getvalue(), "image/png")
            return

        if _connected:
            status = b"Bot is running and connected to WhatsApp"
        else:
            status = b"Bot is running. Not connected yet: open /qr to scan the code"
        self._reply(200, status, "text/plain; charset=utf-8")

    def _reply(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args) -> None:  # без лишнего лога HTTP-запросов
        return


def _run_web_server() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), _WebHandler)
    log.info("Веб-сервер запущен на порту %d", PORT)
    server.serve_forever()


# ============================================================================
# ЗАПУСК
# ============================================================================

if __name__ == "__main__":
    threading.Thread(target=_cleanup_loop, daemon=True).start()
    threading.Thread(target=_run_web_server, daemon=True).start()
    log.info("Запуск бота. Для входа откройте /qr на адресе сервиса.")
    client.connect()  # блокирующий вызов, держит соединение
