import os
import re
import difflib
import calendar
from typing import Dict, Optional, List, Tuple
from datetime import datetime, timedelta, date, time as dtime

import pytz
import gspread
from google.oauth2.service_account import Credentials   # ✅ новый импорт
from gspread_formatting import CellFormat, Color, format_cell_range

from telegram import Update, Bot
from telegram.ext import (
    Updater,
    MessageHandler,
    Filters,
    CommandHandler,
    CallbackContext,
    PollAnswerHandler
)

from dotenv import load_dotenv
load_dotenv()

# ================== КОНФИГ ==================
TELEGRAM_TOKEN     = "8328328658:AAFhgk7dZtCs_FbIFCRVRY2oQvZPiO8BVDo"
SHEET_URL          = "https://docs.google.com/spreadsheets/d/11cbXP_A30Oa_ldjWyNrfCy0dHYMFacgj_5cg-EdjpGk/edit?gid=0#gid=0"

SPREADSHEET_NAME   = (os.getenv("Botprojects") or "").strip()

# ✅ твой ключ оставляем как есть
GSPREAD_CREDS_JSON = {
  "type": "service_account",
  "project_id": "applied-fusion-473621-f6",
  "private_key_id": "86c2fe0d21b7c6f72564de31832b51a2f56f8d62",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCxni8Dqs/eHMpM\n65ksC37lTEtfBETZdKP5JRkY0D/d6nclgLNacTFeXh+4AVAE2F8NmiKMeOz1EbcV\nWs076FBNsI+FKN0+4X56IvdUtcz6tN4JVKU6ms2yC8JK7LxQuOu5v/nE3wkgM7An\nOyEjCOnLvccU+qEv92eKQ0AXxNsMaXRhMUPXLhb3SPhibqiZTs+tzotkI/LqJMg7\nPJWity+3UqtfKTQ6wTDqbcu7MKikCltISeAuwX8ahuxvNA+JqJhGi1g45nQpAOjf\nGuNkPt9JDyJZul7I/ocAvBX5p+1Teqw9BUjM0MXf4EZKb/1GpJXgNdSGM1brBral\n1OhzfAOlAgMBAAECggEADl/2/hz85cT+umr24pGrm3MpRm9aAx9lhexys4dnI9at\n/eMEoPU4QaLsfj/c8gdw6fDniubehpcAckH/CjIrDZ6UfvpnrYcibVAXJEvyYvfw\nYGDxXmTY8OfyuY2fUAknSsk2tbA3VNvaDdLDQ8qc4VhinFgMY41qW85Fiabs83wS\n4K2g/Jm2VNVNF/v2QHKnAxlFUvnKSx9ty+wSSQffAmxXT8vTtgyIq0pd+GkODyhG\npBExB9JU9PTZsYyQj2StNTMRVjTPtjeSpogA/4Wl2PMTxYBVwfApK5vPoQHz+LRW\n8XNGcSImYQQSW1OGhPpxizV6gNY17i2ng0RPZChJoQKBgQDaVXdrTgki8UDEKPYF\nFJUlfv08jUBjPDlndMKySJFBzJExmbAqOUPpG4oUPsOY+4lsAef3/TPvoijZJ5U5\n8pJh6dWytT3GrlZHulazuE/S1YQxtUEpTyRpcqRk6SJcMSU4Yv/bz5ddeSjXqXM+\nnC/H8H4xoc0eW5aKePf1DIZP1wKBgQDQQof0nXqH+qi9CxXgUCs2EXVj3wpOL/XC\nTYd9K1K3v7v8d7XgmZw7de89Vj8vsjyycaLqdh7l/wmUUoQXCkj1Zx8hAytJtozb\nJ1dSchShLTk9dAKC+iVCcOSRK3lbVaxY7f4YNx0+jA1f/Db123KPHI4LnO6VBKWR\nrmqHpq+I4wKBgDY3VuosfJRSuDR9v2nPjKG9AQFSShTaVZ7dHaPL5VYjiEJ62YYC\nplxyXD1ewI9yltNdPc8U7xqod+BEtgentrXdrRUtcGOv0vkIypLiR+Ag6Sy2x7GM\nV4xfzXbJdaDTC10PKF3bFMk/VucGyvlXK35It+13MkpBkAZet6QAMjINAoGBALhU\nCq2RrOAqFaus4iH2Eyj57uMEiMSHYoglwuQgskf8plBhTFOM4mEmkyfA7JA0u9Bd\nEAthnRuIzlu/ZTZXXhgGu+CmQ2ws7SMUAQ/x9RxrQJyJz6dJ7CyQa12qEvGqNK9J\nhnq8XV/86eGpBKQ7JXxRk8/niKvQvxLgQ13pRLhtAoGAL7iyUpzCMjZ2BT8yPmt1\nr0mjlw1jhEK0c5czhqmduHRVNoUsrms9+OicYYRNgsqGaCmLLhE5LvVp4G4ezg1M\nteIdn9I2tFYpsdP9kv/XK8ckLv/SkMA/2+xVMoN5ltfjaNTkOqB/3ckPcb4q7Lgd\nV1ZYah+55Iwt8gsnONJexrw=\n-----END PRIVATE KEY-----\n",
  "client_email": "botprojects@applied-fusion-473621-f6.iam.gserviceaccount.com",
  "client_id": "100385385956228046111",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/botprojects%40applied-fusion-473621-f6.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

GENERAL_CHAT_ID    = 0
BOSS_USERNAME      = (os.getenv("BOSS_USERNAME") or "ekainovskaya").strip()
MANAGER_USERNAMES = ["Silver_Serf4r", "manager2", "manager3"]

AUTO_RELOAD        = True
if not TELEGRAM_TOKEN or not SHEET_URL:
    raise RuntimeError("Не заданы TELEGRAM_TOKEN и/или SHEET_URL (см. .env).")

MOSCOW_TZ = pytz.timezone("Europe/Moscow")

# ================== АВТОРИЗАЦИЯ GOOGLE ==================


# username (без @) -> отображаемое имя
MANAGERS: Dict[str, str] = {
    'prmtanya87': 'Татьяна',
    'nchernobai': 'Надя',
    'ekainovskaya': 'Евгения',
}

# Диапазоны менеджеров на «Проекты»
MANAGER_RANGES: Dict[str, str] = {
    'prmtanya87': 'A2:H200',
    'nchernobai': 'A201:H400',
    'ekainovskaya': 'A401:H600',
}

MANAGER_COLORS: Dict[str, Color] = {
    'prmtanya87': Color(1, 0.9, 0.9),
    'nchernobai': Color(0.9, 1, 0.9),
    'ekainovskaya': Color(0.9, 0.9, 1),
}

# Диапазоны менеджеров на «Отчёты»
REPORTS_MANAGER_RANGES: Dict[str, str] = {
    'prmtanya87': 'A2:E200',
    'nchernobai': 'A201:E400',
    'ekainovskaya': 'A401:E600',
}
REPORTS_MANAGER_COLORS: Dict[str, Color] = {
    'prmtanya87': Color(1, 0.95, 0.9),
    'nchernobai': Color(0.95, 1, 0.9),
    'ekainovskaya': Color(0.9, 0.95, 1),
}

# Цвета дедлайнов
COLOR_ORANGE = Color(1, 0.8, 0.6)  # < 7 дней
COLOR_RED    = Color(1, 0.6, 0.6)  # просрочка

# Этапы (по содержимому сообщения)
PROJECT_STEPS = {
    'приветствует вас': 'Чат создан',
    'начинаем подбор': 'Подбор',
    'блогеры подобраны': 'Выход реклам',
    'рекламной кампании подошла к концу': 'Выслан финальный отчет',
}
FIRST_STEP = 'Чат создан'
LAST_STEP  = 'Выслан финальный отчет'

# Листы
SHEET_MAIN    = "Проекты"
SHEET_HISTORY = "История"
SHEET_ARCHIVE = "Архив"

# Колонки «Проекты»: A..H
COL_DATE, COL_MANAGER, COL_PROJECT, COL_STAGE, COL_DEADLINE, COL_SHEET, COL_REASON, COL_PRODUCT = range(1, 9)

# Чек-лист для ежедневного опроса
REPORT_POLL_OPTIONS = [
    "Контент согласован",
    "Ссылки/UTM проверены",
    "Публикации идут по плану",
    "Оплата проведена/в процессе",
    "Отчётность обновлена",
    "Есть риски/задержки",
    "Нужна помощь/эскалация"
]

# Создание клиента
creds = Credentials.from_service_account_info(GSPREAD_CREDS_JSON, scopes=[
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
])

client = gspread.authorize(creds)
spreadsheet = client.open_by_url(SHEET_URL)

# Функция получения или создания листа
def _get_or_create_ws(ss, title, header=None):
    try:
        ws = ss.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=title, rows=2000, cols=20)
        if header:
            ws.append_row(header)
    return ws

main_sheet = _get_or_create_ws(spreadsheet, "Projects",
                               ["Дата/начала", "Менеджер", "Проект", "История", "Срок", "Ссылка", "Причина", "Товар"])
reports_sheet = _get_or_create_ws(spreadsheet, "Отчёты",
                                  ["Дата/время", "Менеджер", "Проект", "Тип", "Текст/интеграция"])
state_sheet   = _get_or_create_ws(spreadsheet, "_state",
                                  ["Проект", "Дата", "Блогер", "Интеграция", "Announced"])
chats_sheet   = _get_or_create_ws(spreadsheet, "_chats",
                                  ["Проект", "ChatID"])
history_index_sheet = _get_or_create_ws(spreadsheet, "_history_index",
                                        ["Проект", "StartRow"])

# ================== ХРАНИЛКИ ==================
MANAGER_IDS: Dict[str, int] = {}
BOSS_USER_ID: Optional[int] = None
PROJECT_CHAT_IDS: Dict[str, int] = {}

URL_RE = re.compile(r'(https?://\S+)', re.I)
HYPERLINK_RE = re.compile(r'^\s*=\s*HYPERLINK\(\s*"([^"]+)"\s*;', re.I)  # RU локаль: ; как разделитель

# Подтянуть существующие chat_id из _chats
try:
    for row in chats_sheet.get_all_values()[1:]:
        if len(row) >= 2 and row[0] and row[1]:
            PROJECT_CHAT_IDS[row[0]] = int(row[1])
except Exception:
    pass

# Запросы в ЛС: user_id -> {type, project, chat_id, ...}
pending_requests: Dict[int, Dict] = {}

# ================== УТИЛИТЫ ==================
def norm_username(username: Optional[str]) -> str:
    return (username or "").lstrip("@").lower()

def get_manager_display(username_no_at: str) -> str:
    return MANAGERS.get(username_no_at, '@' + username_no_at if username_no_at else '')

def get_username_by_display(display: str) -> Optional[str]:
    for uname, disp in MANAGERS.items():
        if disp == display:
            return uname
    return None

# ============================================================
#      ИСТОРИЯ ПРОЕКТА — НЕПРЕРЫВНЫЕ БЛОКИ, МЯГКОЕ РАСШИРЕНИЕ
# ============================================================
HISTORY_GROW_STEP = 1  # вставляем ровно столько строк, сколько нужно (обычно 1)

def _history_get_index_rows() -> List[List[str]]:
    try:
        return history_index_sheet.get_all_values()[1:]
    except Exception:
        return []

def _history_get_block_start(project_name: str) -> Optional[int]:
    for r in _history_get_index_rows():
        if len(r) >= 2 and r[0] == project_name and r[1].strip():
            try:
                return int(r[1])
            except Exception:
                return None
    return None

def _history_set_block_start(project_name: str, start_row: int):
    rows = history_index_sheet.get_all_values()
    for i, r in enumerate(rows[1:], start=2):
        if r and r[0] == project_name:
            history_index_sheet.update([[project_name, str(start_row)]], f"A{i}:B{i}")
            return
    history_index_sheet.append_row([project_name, str(start_row)])

def _history_get_all_blocks_sorted() -> List[Tuple[str, int]]:
    rows = _history_get_index_rows()
    out: List[Tuple[str, int]] = []
    for r in rows:
        if len(r) >= 2 and r[0] and r[1].strip().isdigit():
            out.append((r[0], int(r[1])))
    return sorted(out, key=lambda x: x[1])

def _history_next_block_start(project_name: str) -> Optional[int]:
    blocks = _history_get_all_blocks_sorted()
    for i, (p, start) in enumerate(blocks):
        if p == project_name:
            if i + 1 < len(blocks):
                return blocks[i + 1][1]
            return None
    return None

def _history_allocate_block(project_name: str) -> int:
    """Новый блок в конце листа: пустая + заголовок + первая строка для записей."""
    all_vals = history_sheet.get_all_values()
    last_row = len(all_vals)
    sep_row = last_row + 1
    header_row = last_row + 2
    history_sheet.update([[""]], f"A{sep_row}:A{sep_row}")
    history_sheet.update([["История проекта:", project_name, "", ""]], f"A{header_row}:D{header_row}")
    start_row = header_row + 1
    _history_set_block_start(project_name, start_row)
    return start_row

def _history_first_empty_in_range(start_row: int, end_row: int) -> Optional[int]:
    if end_row < start_row:
        return start_row
    values = history_sheet.get(f"A{start_row}:D{end_row}")
    for i, row in enumerate(values, start=start_row):
        if not any((c or "").strip() for c in row):
            return i
    return None

def _history_shift_blocks_below(insert_at_row: int, how_many: int):
    """Сдвигаем вниз все блоки, начинающиеся на/ниже insert_at_row, и обновляем индекс."""
    rows = history_index_sheet.get_all_values()
    for i, r in enumerate(rows[1:], start=2):
        if len(r) < 2 or not r[1].strip().isdigit():
            continue
        sr = int(r[1])
        if sr >= insert_at_row:
            history_index_sheet.update([[r[0], str(sr + how_many)]], f"A{i}:B{i}")

def _history_ensure_capacity(project_name: str, start_row: int, need_rows: int = 1) -> int:
    """Ищем первую свободную строку в блоке. Если блок упёрся в следующий — вставляем
       НУЖНОЕ количество строк прямо перед следующим блоком и сдвигаем вниз остальные."""
    next_start = _history_next_block_start(project_name)
    end_row = (next_start - 1) if next_start else len(history_sheet.get_all_values()) + 200
    first_free = _history_first_empty_in_range(start_row, end_row)
    if first_free is not None:
        return first_free

    # свободных нет — расширяем мягко ровно на need_rows
    how_many = max(need_rows, HISTORY_GROW_STEP)
    insert_at = next_start if next_start else (end_row + 1)
    try:
        history_sheet.insert_rows([[]] * how_many, row=insert_at)
    except Exception:
        for _ in range(how_many):
            history_sheet.insert_row([], index=insert_at)
    _history_shift_blocks_below(insert_at, how_many)
    return insert_at

def add_history(project_name: str, manager_name: str, step_name: str) -> str:
    """Пишем запись строго в блок проекта, блок при необходимости расширяется на месте.
       Возвращаем кликабельный HYPERLINK(';') на строку записи."""
    start_row = _history_get_block_start(project_name)
    if not start_row:
        start_row = _history_allocate_block(project_name)
    target_row = _history_ensure_capacity(project_name, start_row, need_rows=1)
    payload = [[
        datetime.now().strftime('%d.%m.%Y %H:%M'),
        project_name,
        manager_name,
        step_name
    ]]
    history_sheet.update(payload, f"A{target_row}:D{target_row}", value_input_option='USER_ENTERED')
    a1 = f"A{target_row}:D{target_row}"
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}/edit#gid={history_sheet.id}&range={a1}"
    return f'=HYPERLINK("{url}"; "{step_name}")'  # RU: ';' как разделитель

# ================== ДРУГИЕ УТИЛИТЫ ==================
def extract_sheet_link_from_pinned(pinned_message_text: str) -> str:
    m = URL_RE.search(pinned_message_text or "")
    return m.group(0) if m else ""

def format_row_color(row_index: int, cell_fmt: Optional[CellFormat]):
    try:
        if cell_fmt:
            format_cell_range(main_sheet, f"A{row_index}:H{row_index}", cell_fmt)
    except Exception:
        pass

def days_left(deadline_str: str) -> Optional[int]:
    try:
        dt = datetime.strptime(deadline_str, '%d.%m.%Y').date()
        return (dt - date.today()).days
    except Exception:
        return None

def color_for_deadline(deadline_str: str, manager_username: str) -> Optional[CellFormat]:
    left = days_left(deadline_str)
    if left is None:
        return None
    if left < 0:
        return CellFormat(backgroundColor=COLOR_RED)
    if left < 7:
        return CellFormat(backgroundColor=COLOR_ORANGE)
    base = MANAGER_COLORS.get(manager_username)
    return CellFormat(backgroundColor=base) if base else None

def sort_manager_block(username_no_at: str):
    """Сортируем блок менеджера по E (срок)."""
    if username_no_at not in MANAGER_RANGES:
        return
    rng = MANAGER_RANGES[username_no_at]
    start, end = rng.split(":")
    start_row = int(''.join(filter(str.isdigit, start)))
    end_row = int(''.join(filter(str.isdigit, end)))
    values = main_sheet.get(f"A{start_row}:H{end_row}")
    def keyfunc(row):
        try:
            return datetime.strptime((row[COL_DEADLINE-1] or ""), "%d.%m.%Y")
        except Exception:
            return datetime.max
    values_sorted = sorted(values, key=keyfunc)
    for i, row in enumerate(values_sorted, start=start_row):
        main_sheet.update([row], f"A{i}:H{i}")

def get_project_row(project_name: str) -> Optional[int]:
    try:
        all_projects = main_sheet.col_values(COL_PROJECT)
        return all_projects.index(project_name) + 1
    except ValueError:
        return None

# ====== «Отчёты» ======
def _reports_find_first_empty(rng: str) -> int:
    start, end = rng.split(":")
    start_row = int(''.join(filter(str.isdigit, start)))
    end_row   = int(''.join(filter(str.isdigit, end)))
    values = reports_sheet.get(f"A{start_row}:E{end_row}")
    for i, row in enumerate(values, start=start_row):
        if not any((cell or "").strip() for cell in row):
            return i
    return end_row

def _reports_sort_block(rng: str):
    start, end = rng.split(":")
    start_row = int(''.join(filter(str.isdigit, start)))
    end_row   = int(''.join(filter(str.isdigit, end)))
    values = reports_sheet.get(f"A{start_row}:E{end_row}")
    def to_dt(v):
        try:
            return datetime.strptime((v or ""), '%d.%m.%Y %H:%M')
        except Exception:
            return datetime.min
    filled = [r for r in values if any((c or "").strip() for c in r)]
    emptyn = len(values) - len(filled)
    filled_sorted = sorted(filled, key=lambda r: to_dt(r[0] if len(r) > 0 else ""), reverse=True)
    data = filled_sorted + [[""]*5 for _ in range(emptyn)]
    reports_sheet.update(data, f"A{start_row}:E{end_row}")

def report_append(manager_username_no_at: str, project: str, kind: str, text: str):
    disp = get_manager_display(manager_username_no_at)
    rng  = REPORTS_MANAGER_RANGES.get(manager_username_no_at)
    color= REPORTS_MANAGER_COLORS.get(manager_username_no_at)
    row_data = [
        datetime.now().strftime('%d.%m.%Y %H:%M'),  # A
        disp,                                       # B
        project,                                    # C
        kind,                                       # D
        text                                        # E
    ]
    if rng:
        row_index = _reports_find_first_empty(rng)
        reports_sheet.update([row_data], f"A{row_index}:E{row_index}")
        if color:
            format_cell_range(reports_sheet, f"A{row_index}:E{row_index}", CellFormat(backgroundColor=color))
        _reports_sort_block(rng)
    else:
        reports_sheet.append_row(row_data)

def notify_boss(context: CallbackContext, text: str):
    global BOSS_USER_ID
    if BOSS_USER_ID:
        try:
            context.bot.send_message(chat_id=BOSS_USER_ID, text=text); return
        except Exception:
            pass
    if GENERAL_CHAT_ID:
        try:
            context.bot.send_message(chat_id=GENERAL_CHAT_ID, text=f"[Руководителю] {text}")
        except Exception:
            pass

def ask_in_dm(context: CallbackContext, user_id: int, text: str) -> bool:
    try:
        context.bot.send_message(chat_id=user_id, text=text)
        return True
    except Exception:
        return False

# ===================== Выбор лучшего листа «Итоги» =====================
import calendar
# re уже импортирован выше

_DATE_REs = [
    re.compile(r'(?P<y>20\d{2})[-_. ](?P<m>0?[1-9]|1[0-2])[-_. ](?P<d>0?[1-9]|[12]\d|3[01])'), # YYYY-MM-DD
    re.compile(r'(?P<d>0?[1-9]|[12]\d|3[01])[-_. ](?P<m>0?[1-9]|1[0-2])[-_. ](?P<y>20\d{2})'), # DD.MM.YYYY
    re.compile(r'(?P<y>20\d{2})[-_. ](?P<m>0?[1-9]|1[0-2])\b'),                                 # YYYY.MM
    re.compile(r'(?P<m>0?[1-9]|1[0-2])[-_. ](?P<y>20\d{2})\b'),                                 # MM.YYYY
    re.compile(r'\b(?P<y>20\d{2})\b'),                                                          # YYYY
]
_SUMMARY_KEYWORDS_STRONG = {"итоги"}
_SUMMARY_KEYWORDS_WEAK   = {"итог", "summary", "dashboard"}

def _parse_date_from_title(title: str) -> Optional[date]:
    t = (title or "").lower()
    for rx in _DATE_REs:
        m = rx.search(t)
        if not m:
            continue
        y = int(m.groupdict().get("y") or 0)
        mth = int(m.groupdict().get("m") or 1)
        d = int(m.groupdict().get("d") or 1)
        if "d" not in m.groupdict():  # если нет дня — берём последний день месяца/года
            if "m" in m.groupdict():
                d = calendar.monthrange(y, mth)[1]
            else:
                mth, d = 12, 31
        try:
            return date(y, mth, d)
        except Exception:
            continue
    return None

def _metric_aliases() -> Dict[str, List[str]]:
    return {
        "охват":   ["охват", "reach"],
        "cpm":     ["cpm", "срм", "стоимость за 1000", "cost per mille", "за 1000 показов"],
        "cpc":     ["cpc", "срс", "стоимость клика", "cost per click"],
        "roi":     ["roi", "окупаемость", "return on investment"],
        "выручка": ["выручка", "доход", "revenue"],
    }

def _count_metric_hits(ws) -> int:
    """Грубая оценка: сколько «меток метрик» встречаем в первых 50x10 ячейках."""
    try:
        data = ws.get('A1:J50', value_render_option='FORMATTED_VALUE')
    except Exception:
        return 0
    if not data:
        return 0
    flat = [str(c).strip().lower() for row in data for c in row if str(c).strip()]
    aliases = _metric_aliases()
    hits = 0
    for key, words in aliases.items():
        for w in words:
            w = w.lower()
            if any(w == cell or w in cell for cell in flat):
                hits += 1
                break
    return hits

def _summary_name_priority(title: str) -> int:
    t = (title or "").lower()
    if t in _SUMMARY_KEYWORDS_STRONG or any(k in t for k in _SUMMARY_KEYWORDS_STRONG):
        return 2
    if any(k in t for k in _SUMMARY_KEYWORDS_WEAK):
        return 1
    return 0

def pick_best_summary_ws(proj_ss):
    """
    Выбираем лучший лист «итогов»:
    1) по дате в названии (новее — лучше),
    2) по приоритету имени (Итоги > содержит итог/summary/dashboard > прочее),
    3) по количеству «меточных» слов в таблице (первые 50x10),
    4) при равенстве — выигрывает самый левый лист (меньший ws.index).
    """
    try:
        wss = proj_ss.worksheets()
    except Exception:
        return None
    candidates = []
    for ws in wss:
        title = ws.title
        dt = _parse_date_from_title(title) or date(1970, 1, 1)
        pri = _summary_name_priority(title)
        hits = _count_metric_hits(ws) if pri > 0 else 0
        candidates.append((dt, pri, hits, ws))
    if not candidates:
        return None
    # дата ↓, приоритет ↓, попадания ↓, индекс ↑ (левее лучше -> меньший index)
    candidates.sort(key=lambda x: (x[0], x[1], x[2], -x[3].index), reverse=True)
    return candidates[0][3]

# ===== Метрики «Итоги» =====
def pull_metrics_from_summary_sheet(sheet_url: str) -> Dict[str, Optional[str]]:
    metrics = {"охват": None, "cpm": None, "cpc": None, "roi": None, "выручка": None}
    if not sheet_url:
        return metrics
    try:
        proj_ss = client.open_by_url(sheet_url)
    except Exception:
        return metrics

    # ВЫБОР ЛУЧШЕГО ЛИСТА
    ws = pick_best_summary_ws(proj_ss)
    if ws is None:
        return metrics

    try:
        data = ws.get_all_values()
    except Exception:
        return metrics
    data = [row[:30] for row in data[:100]]
    if not data:
        return metrics

    aliases = _metric_aliases()
    flat = {k: [s.lower() for s in v] for k, v in aliases.items()}

    def norm(s: str) -> str:
        return (s or "").strip().lower()

    def fuzzy(label: str, key: str) -> bool:
        cand = norm(label)
        if any(cand == a for a in flat[key]):
            return True
        return difflib.get_close_matches(cand, flat[key], n=1, cutoff=0.82) != []

    rows = len(data)
    for r in range(rows):
        cols = len(data[r])
        for c in range(cols):
            cell = data[r][c]
            if not cell:
                continue
            for key in metrics.keys():
                if metrics[key] is not None:
                    continue
                if not fuzzy(cell, key):
                    continue
                if c + 1 < cols and (data[r][c+1] or "").strip():
                    metrics[key] = data[r][c+1]; continue
                if r + 1 < rows and c < len(data[r+1]) and (data[r+1][c] or "").strip():
                    metrics[key] = data[r+1][c]
    if not all(metrics.values()) and rows > 0:
        header = [norm(x) for x in data[0]]
        for key in list(metrics.keys()):
            if metrics[key] is not None:
                continue
            best_j = None; best_sim = 0
            for j, h in enumerate(header):
                if not h:
                    continue
                if h in flat[key]:
                    best_j = j; break
                sim = difflib.SequenceMatcher(a=h, b=flat[key][0]).ratio()
                if sim > best_sim:
                    best_sim = sim; best_j = j
            if best_j is not None:
                for r in range(1, rows):
                    if best_j < len(data[r]) and (data[r][best_j] or "").strip():
                        metrics[key] = data[r][best_j]; break
    return metrics

def archive_project(row_index: int):
    """Перенос в «Архив» + метрики «Итоги», затем удаление строки."""
    row_vals = main_sheet.row_values(row_index)
    sheet_link = row_vals[COL_SHEET-1] if len(row_vals) >= COL_SHEET else ""
    metrics = pull_metrics_from_summary_sheet(sheet_link)
    archive_row = row_vals + [
        metrics.get("охват") or "",
        metrics.get("cpm") or "",
        metrics.get("cpc") or "",
        metrics.get("roi") or "",
        metrics.get("выручка") or ""
    ]
    archive_sheet.append_row(archive_row)
    main_sheet.delete_row(row_index)

# ===== Просрочка — мгновенная проверка =====
def trigger_overdue_check_immediate(context: CallbackContext, row_index: int, manager_username: str):
    """Если E просрочен — сразу спрашиваем причину в ЛС, красим, уведомляем руководителя."""
    try:
        vals = main_sheet.get(f"A{row_index}:H{row_index}")[0]
    except Exception:
        return
    project = vals[COL_PROJECT-1] if len(vals) >= COL_PROJECT else ""
    deadline_str = vals[COL_DEADLINE-1] if len(vals) >= COL_DEADLINE else ""
    if not project or not deadline_str:
        return

    left = days_left(deadline_str)
    if left is None or left >= 0:
        return

    uid = MANAGER_IDS.get(manager_username)
    if uid and ask_in_dm(
        context,
        uid,
        f"⛔ «{project}» просрочен на {-left} дн. Укажите причину просрочки:"
    ):
        pending_requests[uid] = {"type": "OVERDUE_REASON", "project": project, "chat_id": None}

    try:
        format_cell_range(main_sheet, f"A{row_index}:H{row_index}", CellFormat(backgroundColor=COLOR_RED))
    except Exception:
        pass
    notify_boss(context, f"Просрочка по «{project}» ({-left} дн.). Запрошена причина у @{manager_username}.")

# ================== ЛОГИКА ПРОЕКТОВ ==================
def upsert_project_row(context: CallbackContext,
                       project_name: str, manager_username: str, stage_name: str,
                       sheet_link: str, deadline_date: Optional[datetime],
                       reason: str = "", product: Optional[str] = None,
                       start_date_str: Optional[str] = None) -> int:
    """Создаёт/обновляет проект, пишет историю, сохраняет товар, вызывает проверку просрочки.
       start_date_str -> колонка A (например, дата передачи)."""
    manager_name = get_manager_display(manager_username)
    hist_link = add_history(project_name, manager_name, stage_name)

    row = get_project_row(project_name)

    # Прежние E(срок), H(товар) — чтобы не затирать
    old_deadline_str, old_product = "", ""
    if row:
        try:
            old_vals = main_sheet.get(f"E{row}:H{row}")[0]  # [E, F, G, H]
            old_deadline_str = (old_vals[0] or "").strip()
            old_product      = (old_vals[3] or "").strip()
        except Exception:
            pass

    final_deadline_str = deadline_date.strftime('%d.%m.%Y') if deadline_date else (old_deadline_str or "")
    final_product = (product if (product is not None and product != "") else (old_product or ""))

    values = [
        (start_date_str or datetime.now().strftime('%d.%m.%Y')),  # A
        manager_name,                           # B
        project_name,                           # C
        hist_link,                              # D (кликабельный HYPERLINK)
        final_deadline_str,                     # E
        sheet_link,                             # F
        reason or "",                           # G
        final_product                           # H (товар сохраняем)
    ]

    # Записываем строку
    if row:
        main_sheet.update([values], f"A{row}:H{row}", value_input_option='USER_ENTERED')
    else:
        main_sheet.append_row(values, value_input_option='USER_ENTERED')
        row = len(main_sheet.get_all_values())

    # === СРАЗУ архивируем, если это последний этап (до подсветки/сортировки) ===
    try:
        if stage_name.strip().lower() == LAST_STEP.lower():
            archive_project(row)
            return row  # строка уже удалена
    except Exception:
        pass

    # Подсветка + сортировка
    fmt = color_for_deadline(values[COL_DEADLINE-1], manager_username)
    if fmt:
        format_cell_range(main_sheet, f"A{row}:H{row}", fmt)
    sort_manager_block(manager_username)

    # Мгновенная проверка просрочки
    try:
        trigger_overdue_check_immediate(context, row, manager_username)
    except Exception:
        pass

    return row

def detect_step(text: str) -> Optional[str]:
    low = (text or "").lower()
    for key, stage in PROJECT_STEPS.items():
        if key in low:
            return stage
    return None

# ================== ПЛАН (сегодня) ==================
SCHEDULE_SHEET_CANDIDATES = ["Джем", "Ручная", "Schedule", "Calendar"]
SCHEDULE_ALIASES = {
    "date":   ["дата", "дата интеграции", "date", "integration date"],
    "blogger":["блогер", "канал", "influencer", "blogger", "creator"],
    "link":   ["интеграция", "ссылка", "url", "link"],
}

def _norm(s: str) -> str:
    return (s or "").strip().lower()

def _col_by_header(headers: list, alias_list: list) -> int:
    headers_n = [_norm(h) for h in headers]
    alias_n = [_norm(a) for a in alias_list]
    for i, h in enumerate(headers_n):
        if h in alias_n:
            return i
    best_i, best = -1, 0
    for i, h in enumerate(headers_n):
        for a in alias_list:
            sim = difflib.SequenceMatcher(a=_norm(h), b=_norm(a)).ratio()
            if sim > best and sim >= 0.8:
                best, best_i = sim, i
    return best_i

def _parse_date_cell(value: str) -> Optional[date]:
    v = (value or "").strip()
    if not v:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(v, fmt).date()
        except Exception:
            pass
    try:
        serial = float(v.replace(",", "."))
        base = date(1899, 12, 30)
        return base + timedelta(days=int(serial))
    except Exception:
        return None

def _find_schedule_ws(proj_ss):
    for title in SCHEDULE_SHEET_CANDIDATES:
        try:
            return proj_ss.worksheet(title)
        except Exception:
            continue
    try:
        return proj_ss.worksheets()[0]
    except Exception:
        return None

def _extract_url_from_cell_display_or_formula(display: str, formula: str) -> str:
    m = HYPERLINK_RE.match(formula or "")
    if m:
        return m.group(1)
    m2 = URL_RE.search(display or "")
    return m2.group(0) if m2 else ""

def scan_today_rows_with_links(sheet_url: str, today: date) -> list:
    out = []
    if not sheet_url:
        return out
    try:
        ss = client.open_by_url(sheet_url)
    except Exception:
        return out
    ws = _find_schedule_ws(ss)
    if not ws:
        return out

    try:
        display = ws.get('A1:Z200', value_render_option='FORMATTED_VALUE')
        formula = ws.get('A1:Z200', value_render_option='FORMULA')
    except Exception:
        return out
    if not display:
        return out

    header = display[0]
    i_date = _col_by_header(header, SCHEDULE_ALIASES["date"])
    i_blog = _col_by_header(header, SCHEDULE_ALIASES["blogger"])
    i_link = _col_by_header(header, SCHEDULE_ALIASES["link"])
    if i_date == -1:
        return out

    for r in range(1, len(display)):
        row_disp = display[r]
        row_frm  = formula[r] if r < len(formula) else []
        if i_date >= len(row_disp) or not row_disp[i_date]:
            continue
        d = _parse_date_cell(row_disp[i_date])
        if d != today:
            continue

        blogger_disp = row_disp[i_blog] if (i_blog != -1 and i_blog < len(row_disp)) else ""
        blogger_formula = row_frm[i_blog] if (i_blog != -1 and i_blog < len(row_frm)) else ""
        url = _extract_url_from_cell_display_or_formula(blogger_disp, blogger_formula)

        if not url and i_link != -1:
            link_disp = row_disp[i_link] if i_link < len(row_disp) else ""
            link_form = row_frm[i_link] if i_link < len(row_frm) else ""
            url = _extract_url_from_cell_display_or_formula(link_disp, link_form)

        out.append({"blogger_disp": blogger_disp, "link": url, "raw_row": row_disp})
    return out

# ====== СОСТОЯНИЕ публикаций ======
def _state_find_row(project: str, date_iso: str, blogger: str) -> Optional[int]:
    rows = state_sheet.get_all_values()
    for idx, row in enumerate(rows[1:], start=2):
        if len(row) < 5:
            continue
        if row[0] == project and row[1] == date_iso and row[2] == blogger:
            return idx
    return None

def state_was_announced(project: str, day: date, blogger: str, link: str) -> bool:
    idx = _state_find_row(project, day.isoformat(), blogger)
    if not idx:
        return False
    row = state_sheet.row_values(idx)
    announced = (row[4] == "1")
    if link and (len(row) < 4 or row[3] != link):
        state_sheet.update([[project, day.isoformat(), blogger, link, "1"]], f"A{idx}:E{idx}")
        return True
    return announced

def state_mark_announced(project: str, day: date, blogger: str, link: str):
    idx = _state_find_row(project, day.isoformat(), blogger)
    if idx:
        state_sheet.update([[project, day.isoformat(), blogger, link or "", "1"]], f"A{idx}:E{idx}")
    else:
        state_sheet.append_row([project, day.isoformat(), blogger, link or "", "1"])

# ================== ОБРАБОТЧИКИ ТЕЛЕГРАМ ==================
def handle_pinned_message(update: Update, context: CallbackContext):
    """Закрепили сообщение → тихо читаем URL таблицы, пишем в F и сохраняем ChatID в _chats."""
    msg = update.message
    if not msg or msg.chat.type not in ("group", "supergroup"):
        return

    chat = msg.chat
    chat_title = chat.title or "Без названия"
    pinned = msg.pinned_message
    if not pinned or not pinned.text:
        return

    sheet_link = extract_sheet_link_from_pinned(pinned.text)
    if not sheet_link:
        return

    row = get_project_row(chat_title)
    if row:
        main_sheet.update([[sheet_link]], f"F{row}:F{row}", value_input_option='USER_ENTERED')
    else:
        values = [
            datetime.now().strftime('%d.%m.%Y'),  # A
            "",                                    # B
            chat_title,                            # C
            "",                                    # D
            "",                                    # E
            sheet_link,                            # F
            "",                                    # G
            ""                                     # H
        ]
        main_sheet.append_row(values, value_input_option='USER_ENTERED')
        row = len(main_sheet.get_all_values())

    # сохраняем chat_id
    PROJECT_CHAT_IDS[chat_title] = chat.id
    rows = chats_sheet.get_all_values()
    pos = None
    for i, r in enumerate(rows[1:], start=2):
        if r and r[0] == chat_title:
            pos = i; break
    if pos:
        chats_sheet.update([[chat_title, str(chat.id)]], f"A{pos}:B{pos}")
    else:
        chats_sheet.append_row([chat_title, str(chat.id)])

def handle_group_message(update: Update, context: CallbackContext):
    """В группах ловим этапы. В чат ничего не пишем (кроме отдельной джобы про «интеграция вышла»)."""
    msg = update.message
    if not msg or msg.chat.type not in ("group", "supergroup"):
        return

    chat = msg.chat
    user = msg.from_user
    username_no_at = norm_username(user.username)
    MANAGER_IDS[username_no_at] = user.id
    if username_no_at == norm_username(BOSS_USERNAME):
        global BOSS_USER_ID
        BOSS_USER_ID = user.id

    chat_title = chat.title or "Без названия"

    text = msg.text or ""
    stage_name = detect_step(text)
    if not stage_name:
        return

    # читаем F из закрепа, если есть
    try:
        full_chat = context.bot.get_chat(chat.id)
        pin = full_chat.pinned_message
        sheet_link = extract_sheet_link_from_pinned(pin.text) if (pin and pin.text) else ""
    except Exception:
        sheet_link = ""

    if stage_name == FIRST_STEP:
        deadline = datetime.now() + timedelta(days=30)
        upsert_project_row(context, chat_title, username_no_at, stage_name, sheet_link, deadline, product=None)
        if ask_in_dm(context, user.id, f"Проект «{chat_title}». Что за товар рекламируется? Ответьте одним сообщением."):
            pending_requests[user.id] = {"type": "PRODUCT", "project": chat_title, "chat_id": chat.id}
        return

    row = get_project_row(chat_title)
    if not row:
        if ask_in_dm(context, user.id,
                     f"Проект «{chat_title}» начат не с первого этапа.\nУкажите дату передачи проекта (ДД.ММ.ГГГГ):"):
            pending_requests[user.id] = {
                "type": "TRANSFER_DATE",
                "project": chat_title,
                "chat_id": chat.id,
                "stage": stage_name,
                "sheet_link": sheet_link
            }
        return

    try:
        existing_deadline = (main_sheet.cell(row, COL_DEADLINE).value or "").strip()
    except Exception:
        existing_deadline = ""

    if existing_deadline:
        upsert_project_row(context, chat_title, username_no_at, stage_name, sheet_link, deadline_date=None, product=None)
        return
    else:
        if ask_in_dm(context, user.id, f"Проект «{chat_title}»: укажите дату передачи (ДД.ММ.ГГГГ):"):
            pending_requests[user.id] = {
                "type": "TRANSFER_DATE",
                "project": chat_title,
                "chat_id": chat.id,
                "stage": stage_name,
                "sheet_link": sheet_link
            }
        return

def handle_private_message(update: Update, context: CallbackContext):
    msg = update.message
    if not msg or msg.chat.type != "private":
        return
    user = msg.from_user
    username_no_at = norm_username(user.username)
    MANAGER_IDS[username_no_at] = user.id
    if username_no_at == norm_username(BOSS_USERNAME):
        global BOSS_USER_ID
        BOSS_USER_ID = user.id

    p = pending_requests.get(user.id)
    if not p:
        return

    kind = p.get("type")
    project = p.get("project")

    if kind == "TRANSFER_DATE":
        try:
            transfer = datetime.strptime((msg.text or "").strip(), '%d.%m.%Y')
        except ValueError:
            msg.reply_text("Неверный формат. Пример: 15.09.2025"); return

        deadline = transfer + timedelta(days=30)
        upsert_project_row(
            context,
            project,
            username_no_at,
            p.get("stage"),
            p.get("sheet_link") or "",
            deadline_date=deadline,
            product=None,
            start_date_str=transfer.strftime('%d.%m.%Y')
        )
        msg.reply_text(f"Дата передачи принята. Срок договора: {deadline.strftime('%d.%m.%Y')}.")
        report_append(username_no_at, project, "Дата передачи", transfer.strftime('%d.%m.%Y'))

        if ask_in_dm(context, user.id, "Что за товар рекламируется? Ответьте одним сообщением."):
            pending_requests[user.id] = {"type": "PRODUCT", "project": project, "chat_id": p.get("chat_id")}
        else:
            notify_boss(context, f"Не удалось спросить товар у @{username_no_at} для «{project}».")
        pending_requests.pop(user.id, None); return

    if kind == "APPROVAL_TODAY":
        ans = (msg.text or "").strip().lower()
        ok = ans in ("да", "д", "yes", "y", "ok", "ок")
        report_append(username_no_at, project, "Согласование", "Да" if ok else "Нет")
        pending_requests.pop(user.id, None); return

    if kind == "DAILY_REPORT":
        report = (msg.text or "").strip()
        report_append(username_no_at, project, "Отчёт", report)
        pending_requests.pop(user.id, None); return

    if kind == "LAST_ADS":
        ans = (msg.text or "").strip()
        report_append(username_no_at, project, "Даты последних реклам", ans)
        pending_requests.pop(user.id, None); return

    if kind == "OVERDUE_REASON":
        reason = (msg.text or "").strip()
        row = get_project_row(project)
        if row:
            main_sheet.update_cell(row, COL_REASON, reason)
        report_append(username_no_at, project, "Просрочка (причина)", reason)
        notify_boss(context, f"Причина просрочки по «{project}» от @{username_no_at}: {reason or '(не указана)'}")
        pending_requests.pop(user.id, None); return

    if kind == "PRODUCT":
        product = (msg.text or "").strip()
        row = get_project_row(project)
        if row:
            main_sheet.update([[product]], f"H{row}:H{row}", value_input_option='USER_ENTERED')
            report_append(username_no_at, project, "Товар", product)
        pending_requests.pop(user.id, None); return

    if kind == "DAILY_REPORT_NOTES":
        notes = (msg.text or "").strip()
        report_append(username_no_at, project, "Ежедневный комментарий", notes)
        pending_requests.pop(user.id, None); return

# ====== ОТВЕТЫ НА ОПРОСЫ (Poll) ======
def handle_poll_answer(update: Update, context: CallbackContext):
    pa = update.poll_answer
    if not pa:
        return
    poll_id = pa.poll_id
    user_id = pa.user.id
    chosen_indices = pa.option_ids or []

    meta = context.bot_data.get("report_polls", {}).get(poll_id)
    if not meta:
        return

    uname = meta["manager_username"]
    project = meta["project"]

    chosen_labels = [REPORT_POLL_OPTIONS[i] for i in chosen_indices if 0 <= i < len(REPORT_POLL_OPTIONS)]
    chosen_text = "; ".join(chosen_labels) if chosen_labels else "Нет отметок"

    report_append(uname, project, "Ежедневный опрос", chosen_text)
    try:
        context.bot.send_message(
            chat_id=user_id,
            text="Добавьте короткий комментарий по проекту. Если ничего — ответьте «-»."
        )
        pending_requests[user_id] = {"type": "DAILY_REPORT_NOTES", "project": project, "chat_id": None}
    except Exception:
        pass

    context.bot_data.get("report_polls", {}).pop(poll_id, None)

# ================== ДЖОБЫ ==================
def job_morning_today_ads(context: CallbackContext):
    """09:00 МСК — напоминание менеджерам о сегодняшних интеграциях (ЛС)."""
    rows = main_sheet.get_all_values()
    if not rows:
        return
    today_msk = datetime.now(MOSCOW_TZ).date()

    for row in rows[1:]:
        row = (row + [""]*8)[:8]
        manager_disp, project_name, sheet_link = row[1], row[2], row[5]
        uname = get_username_by_display(manager_disp)
        if not uname:
            continue
        uid = MANAGER_IDS.get(uname)
        today_rows = scan_today_rows_with_links(sheet_link, today_msk)
        if not today_rows:
            continue
        lines = [f"📣 Сегодня выход рекламы по проекту «{project_name}»:"] + [
            f"• {(it['blogger_disp'] or 'Блогер')}: {it['link'] or '(интеграция не указана)'}" for it in today_rows
        ]
        if uid:
            ask_in_dm(context, uid, "\n".join(lines))

def job_noon_approval(context: CallbackContext):
    """12:00 МСК — спросить «согласовано?» по сегодняшним интеграциям (ЛС)."""
    rows = main_sheet.get_all_values()
    if not rows:
        return
    today_msk = datetime.now(MOSCOW_TZ).date()

    for row in rows[1:]:
        row = (row + [""]*8)[:8]
        manager_disp, project_name, sheet_link = row[1], row[2], row[5]
        uname = get_username_by_display(manager_disp)
        if not uname:
            continue
        uid = MANAGER_IDS.get(uname)
        today_rows = scan_today_rows_with_links(sheet_link, today_msk)
        if not today_rows:
            continue
        if uid and ask_in_dm(context, uid, f"✅ «{project_name}»: ролики/посты на сегодня согласованы? Ответьте да/нет."):
            pending_requests[uid] = {"type": "APPROVAL_TODAY", "project": project_name, "chat_id": context.job.context}

def job_evening_report(context: CallbackContext):
    """20:00 МСК — отправляем неанонимный опрос по каждому активному проекту (ЛС)."""
    rows = main_sheet.get_all_values()
    if not rows:
        return
    by_manager = {}
    for row in rows[1:]:
        row = (row + [""]*8)[:8]
        manager_disp, project_name = row[1], row[2]
        uname = get_username_by_display(manager_disp)
        if not uname or not project_name:
            continue
        by_manager.setdefault(uname, []).append(project_name)

    for uname, projects in by_manager.items():
        uid = MANAGER_IDS.get(uname)
        if not uid:
            # логируем, что опрос не доставлен (менеджер не писал боту в ЛС)
            for project in projects:
                report_append(uname, project, "Опрос 20:00", "Не доставлен: менеджер не в ЛС")
            continue
        for project in projects:
            try:
                msg = context.bot.send_poll(
                    chat_id=uid,
                    question=f"Ежедневный отчёт — «{project}»",
                    options=REPORT_POLL_OPTIONS,
                    allows_multiple_answers=True,
                    is_anonymous=False
                )
                context.bot_data.setdefault("report_polls", {})[msg.poll.id] = {
                    "manager_username": uname,
                    "project": project
                }
            except Exception:
                pass

def job_watch_integration_links(context: CallbackContext):
    """Каждые 60 сек: если у сегодняшней интеграции появилась ссылка — сразу в чат проекта + в «Отчёты»."""
    today = datetime.now(MOSCOW_TZ).date()

    rows = main_sheet.get_all_values()
    if not rows:
        return

    for row in rows[1:]:
        row = (row + [""]*8)[:8]
        manager_disp, project_name, sheet_link = row[1], row[2], row[5]
        if not project_name or not sheet_link:
            continue

        chat_id = PROJECT_CHAT_IDS.get(project_name)
        if not chat_id:
            continue

        today_rows = scan_today_rows_with_links(sheet_link, today)
        if not today_rows:
            continue

        for it in today_rows:
            blogger = it["blogger_disp"] or "Блогер"
            url = it["link"]
            if not url:
                continue
            # защита от дублей: отметка в _state
            if state_was_announced(project_name, today, blogger, url):
                continue

            try:
                context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "✅ Сегодняшняя реклама вышла\n"
                        f"Проект: «{project_name}»\n"
                        f"Блогер: {blogger}\n"
                        f"Интеграция: {url}"
                    )
                )
            except Exception:
                notify_boss(context, f"[не отправлено в чат] «{project_name}»: {blogger} — {url}")

            uname = get_username_by_display(manager_disp) or ""
            report_append(uname, project_name, "Публикация", f"{blogger}: {url}")
            state_mark_announced(project_name, today, blogger, url)

def check_project_dates(context: CallbackContext):
    """Раз в сутки: подсветка сроков, напоминания <7 дней, просрочки, перенос финальных в Архив (подстраховка)."""
    rows = main_sheet.get_all_values()
    if not rows:
        return
    for idx, row in enumerate(rows[1:], start=2):
        row = (row + [""]*8)[:8]
        manager_b, project_c, stage_d, end_e = row[1], row[2], row[3], row[4]
        uname = get_username_by_display(manager_b) or ""

        fmt = color_for_deadline(end_e, uname)
        if fmt:
            format_cell_range(main_sheet, f"A{idx}:H{idx}", fmt)

        left = days_left(end_e)
        if left is None:
            continue

        if 0 < left < 7 and uname:
            uid = MANAGER_IDS.get(uname)
            if uid and ask_in_dm(
                context,
                uid,
                f"⚠️ «{project_c}»: до конца договора {left} дн. Пришлите даты последних реклам."
            ):
                pending_requests[uid] = {"type": "LAST_ADS", "project": project_c, "chat_id": context.job.context}
                report_append(uname, project_c, "Запрос дат реклам", f"Осталось {left} дней")

        if left < 0 and uname:
            uid = MANAGER_IDS.get(uname)
            format_cell_range(main_sheet, f"A{idx}:H{idx}", CellFormat(backgroundColor=COLOR_RED))
            if uid and ask_in_dm(
                context,
                uid,
                f"⛔ «{project_c}» просрочен на {-left} дн. Укажите причину просрочки:"
            ):
                pending_requests[uid] = {"type": "OVERDUE_REASON", "project": project_c, "chat_id": context.job.context}

        if stage_d:
            s = stage_d.strip().lower()
            if (LAST_STEP.lower() in s) or ("выслан финальный отчет" in s):
                try:
                    archive_project(idx)
                except Exception:
                    pass

# ================== ЗАПУСК ==================


# Основные таблицы

updater = Updater(token=TELEGRAM_TOKEN, use_context=True)
dp = updater.dispatcher

updater.job_queue.run_daily(
    job_morning_today_ads,
    time=dtime(9, 0, tzinfo=MOSCOW_TZ),
    context=GENERAL_CHAT_ID
)
updater.job_queue.run_daily(
    job_noon_approval,
    time=dtime(12, 0, tzinfo=MOSCOW_TZ),
    context=GENERAL_CHAT_ID
)
updater.job_queue.run_daily(
    job_evening_report,
    time=dtime(20, 0, tzinfo=MOSCOW_TZ),
    context=GENERAL_CHAT_ID
)
# Хендлеры
dp.add_handler(MessageHandler(Filters.chat_type.groups & Filters.status_update.pinned_message, handle_pinned_message))
dp.add_handler(MessageHandler(Filters.chat_type.groups & Filters.text & (~Filters.command), handle_group_message))
dp.add_handler(MessageHandler(Filters.chat_type.private & Filters.text & (~Filters.command), handle_private_message))
dp.add_handler(PollAnswerHandler(handle_poll_answer))

# Плановые задачи (МСК)
updater.job_queue.run_daily(job_morning_today_ads, time=dtime(9, 0),  context=GENERAL_CHAT_ID)
updater.job_queue.run_daily(job_noon_approval,   time=dtime(12, 0), context=GENERAL_CHAT_ID)
updater.job_queue.run_daily(job_evening_report,  time=dtime(20, 0), context=GENERAL_CHAT_ID)

# Мониторинг появления ссылок на интеграции — КАЖДЫЕ 60 СЕК
updater.job_queue.run_repeating(job_watch_integration_links, interval=60, first=5, context=GENERAL_CHAT_ID)

# Суточная проверка сроков и финализации
updater.job_queue.run_repeating(check_project_dates, interval=24*60*60, first=10, context=GENERAL_CHAT_ID)

def main():
    print("Бот запущен…")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()

