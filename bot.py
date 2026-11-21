"""
WhatsApp-бот для автоматизации записи клиентов в баню.
Библиотека: neonize (вход по QR-коду).
Хранение: Firebase Realtime Database (состояния клиентов и резервная копия сессии WhatsApp).
Для Render: встроенный HTTP-сервер (health check и страница с QR-кодом /qr).

Машина состояний (по номеру клиента):
    [нет состояния] --первое сообщение--> WAITING_FOR_DATA
    WAITING_FOR_DATA --данные заявки--> COMPLETED
    Любое состояние сбрасывается через SESSION_TIMEOUT секунд неактивности.
"""

import base64
import io
import json
import logging
import os
import sqlite3
import threading
import time
import urllib.request
from dataclasses import dataclass
from enum import Enum, auto
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional

import qrcode
from neonize.client import NewClient
from neonize.events import ConnectedEv, MessageEv, QREv
from neonize.utils import build_jid


# ============================================================================
# НАСТРОЙКИ
# ============================================================================

# Файл сессии WhatsApp (локальная копия; при перезапуске восстанавливается из Firebase)
SESSION_DB = os.environ.get("SESSION_DB", "db.sqlite3")

# Порт, который выдаёт Render (локально по умолчанию 10000)
PORT = int(os.environ.get("PORT", "10000"))

# Номер администратора: только цифры, с кодом страны, без «+».
# Переменная окружения ADMIN_PHONE, если задана в Render, имеет приоритет.
ADMIN_PHONE = os.environ.get("ADMIN_PHONE", "77787003325")

# Firebase Realtime Database
FIREBASE_URL = os.environ.get(
    "FIREBASE_URL",
    "https://jcord-f0102-default-rtdb.europe-west1.firebasedatabase.app",
)
# Корневой раздел в базе. Лучше задать длинное случайное имя через переменную окружения,
# потому что без авторизации любой, кто знает URL и путь, сможет читать и писать данные.
FIREBASE_PATH = os.environ.get("FIREBASE_PATH", "bath_bot")

# Таймаут неактивности клиента, после которого состояние сбрасывается (секунды)
SESSION_TIMEOUT = 30 * 60
# Как часто сохранять резервную копию сессии WhatsApp (секунды)
SESSION_BACKUP_INTERVAL = 10 * 60
# Через сколько секунд писать предупреждение, если QR-код так и не пришёл
QR_WATCHDOG_DELAY = 60

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
# ЛОГИРОВАНИЕ
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("bath_bot")


# ============================================================================
# FIREBASE REALTIME DATABASE (REST API)
# ============================================================================

def _fb_request(method: str, path: str, payload: Any = None) -> Any:
    """Запрос к Firebase Realtime Database. Возвращает разобранный JSON или None."""
    url = f"{FIREBASE_URL}/{FIREBASE_PATH}/{path}.json"
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = resp.read()
    return json.loads(raw) if raw else None


# ============================================================================
# РЕЗЕРВНАЯ КОПИЯ СЕССИИ WHATSAPP
# ============================================================================

def _backup_session() -> None:
    """Копирует файл сессии (через механизм backup SQLite) и сохраняет его в Firebase."""
    if not os.path.exists(SESSION_DB):
        return
    tmp_path = SESSION_DB + ".bak"
    try:
        src = sqlite3.connect(SESSION_DB)
        dst = sqlite3.connect(tmp_path)
        src.backup(dst)
        dst.close()
        src.close()
        with open(tmp_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("ascii")
        _fb_request("PUT", "wa_session", {"data": encoded, "saved_at": time.time()})
        log.info("Сессия WhatsApp сохранена в Firebase")
    except Exception:
        log.exception("Не удалось сохранить сессию WhatsApp в Firebase")
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def _restore_session() -> None:
    """Если локального файла сессии нет (например, после перезапуска Render), восстанавливает его."""
    if os.path.exists(SESSION_DB):
        log.info("Локальная сессия найдена, восстановление не нужно")
        return
    try:
        stored = _fb_request("GET", "wa_session")
    except Exception:
        log.exception("Не удалось получить сессию из Firebase")
        return
    if not stored or "data" not in stored:
        log.info("Сохранённой сессии в Firebase нет, потребуется сканирование QR-кода")
        return
    with open(SESSION_DB, "wb") as f:
        f.write(base64.b64decode(stored["data"]))
    log.info("Сессия WhatsApp восстановлена из Firebase")


# Восстанавливаем сессию ДО создания клиента, иначе neonize откроет пустой файл
_restore_session()

client = NewClient(SESSION_DB)

# Состояние для веб-страницы с QR-кодом
_latest_qr: Optional[str] = None
_connected = False
_qr_received = False


# ============================================================================
# МАШИНА СОСТОЯНИЙ (хранится в Firebase)
# ============================================================================

class State(Enum):
    WAITING_FOR_DATA = auto()
    COMPLETED = auto()


def _get_state(key: str) -> Optional[State]:
    """Возвращает состояние клиента или None, если его нет или оно истекло."""
    try:
        data = _fb_request("GET", f"sessions/{key}")
    except Exception:
        log.exception("Ошибка чтения состояния клиента %s", key)
        return None
    if not data:
        return None
    if time.time() - float(data.get("last_active", 0)) > SESSION_TIMEOUT:
        _delete_state(key)
        log.info("Состояние %s истекло по таймауту", key)
        return None
    try:
        return State[data["state"]]
    except KeyError:
        return None


def _set_state(key: str, state: State) -> None:
    """Сохраняет состояние клиента и обновляет время последней активности."""
    try:
        _fb_request("PUT", f"sessions/{key}", {"state": state.name, "last_active": time.time()})
    except Exception:
        log.exception("Ошибка записи состояния клиента %s", key)


def _delete_state(key: str) -> None:
    try:
        _fb_request("DELETE", f"sessions/{key}")
    except Exception:
        log.exception("Ошибка удаления состояния клиента %s", key)


# ============================================================================
# ОТПРАВКА СООБЩЕНИЙ
# ============================================================================

def _send(jid: Any, text: str) -> bool:
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

def _extract_qr_code(event: Any) -> Optional[str]:
    """Пробует найти строку с QR-кодом в событии. Имя поля зависит от версии neonize."""
    for name in ("Codes", "codes", "Code", "code", "QRCode", "qr_code"):
        value = getattr(event, name, None)
        if isinstance(value, (list, tuple)) and value:
            return str(value[0])
        if isinstance(value, str) and value:
            return value
    return None


@client.event(QREv)
def on_qr(_: NewClient, event: QREv) -> None:
    global _latest_qr, _qr_received
    # Диагностика: пишем содержимое события, чтобы видеть реальные имена полей
    log.info("Событие QR получено: %r", event)
    code = _extract_qr_code(event)
    if not code:
        log.warning("В событии QR не найдено поле с кодом, см. строку выше")
        return
    _latest_qr = code
    _qr_received = True
    log.info("Получен новый QR-код, откройте /qr в браузере")


@client.event(ConnectedEv)
def on_connected(_: NewClient, __: ConnectedEv) -> None:
    global _latest_qr, _connected
    _connected = True
    _latest_qr = None
    log.info("Бот подключён к WhatsApp")
    threading.Thread(target=_backup_session, daemon=True).start()


@client.event(MessageEv)
def on_message(_: NewClient, message: MessageEv) -> None:
    if _should_ignore(message):
        return

    src = message.Info.MessageSource
    chat = src.Chat
    # Ключ в Firebase: только номер (в ключах RTDB нельзя использовать символ «@»)
    key = chat.User
    client_phone = src.Sender.User

    text = _extract_text(message)
    state = _get_state(key)

    try:
        # Первое сообщение клиента: автоответ и запрос данных для брони
        if state is None:
            _send(chat, WELCOME_TEXT)
            _send(chat, BOOKING_REQUEST_TEXT)
            _set_state(key, State.WAITING_FOR_DATA)
            log.info("Новый клиент %s, ожидаем данные", client_phone)
            return

        # После отправки заявки бот молчит, чтобы не мешать администратору
        if state is State.COMPLETED:
            _set_state(key, State.COMPLETED)
            return

        # Клиент прислал данные заявки
        if state is State.WAITING_FOR_DATA:
            if not text:
                _send(chat, NEED_TEXT_TEXT)
                _set_state(key, State.WAITING_FOR_DATA)
                return

            _send(chat, REQUEST_ACCEPTED_TEXT)
            if _forward_to_admin(client_phone, text):
                log.info("Заявка от %s переслана администратору", client_phone)
            else:
                log.warning("Заявка от %s НЕ переслана, требуется проверка", client_phone)

            _set_state(key, State.COMPLETED)

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
        elif _qr_received:
            status = b"Bot is running. Not connected yet: open /qr to scan the code"
        else:
            status = b"Bot is running. Waiting for QR code from WhatsApp (check Render logs)"
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


def _backup_loop() -> None:
    """Периодически сохраняет сессию WhatsApp в Firebase."""
    while True:
        time.sleep(SESSION_BACKUP_INTERVAL)
        if _connected:
            _backup_session()


def _qr_watchdog() -> None:
    """Если через QR_WATCHDOG_DELAY секунд QR не пришёл, пишет подсказку в лог."""
    time.sleep(QR_WATCHDOG_DELAY)
    if not _connected and not _qr_received:
        log.warning(
            "QR-код не получен за %d с. Проверьте строки «Событие QR получено» выше. "
            "Если их нет, событие не вызывается.",
            QR_WATCHDOG_DELAY,
        )


# ============================================================================
# ЗАПУСК
# ============================================================================

if __name__ == "__main__":
    threading.Thread(target=_run_web_server, daemon=True).start()
    threading.Thread(target=_backup_loop, daemon=True).start()
    threading.Thread(target=_qr_watchdog, daemon=True).start()
    log.info("Запуск бота. Для входа откройте /qr на адресе сервиса.")
    client.connect()  # блокирующий вызов, держит соединение
