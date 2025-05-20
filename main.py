import os
import logging
import random
from typing import Dict, List, Optional
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
import sqlite3
import string
from datetime import datetime
import math

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Инициализация бота
bot = Bot(
    token="8121913607:AAHGPXwGqcJnufM2kyayIvyJA1plBjp192E",
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher(storage=MemoryStorage())

# Настройки бота
CHANNEL_ID = "freewot_acc"  # Username канала без @
CHANNEL_URL = "https://t.me/freewot_acc"  # Прямая ссылка на канал
ADMIN_IDS = [7019824443,929704825]  # ID администраторов
CAPTCHA_LENGTH = 5
FREE_ACCOUNT_CATEGORY = "Бесплатные"  # Категория для бесплатных аккаунтов
INDENT = "                "  # 16 пробелов для красной строки
PRODUCTS_PER_PAGE = 5  # Количество товаров на одной странице

# Инициализация базы данных
try:
    conn = sqlite3.connect('accounts_bot.db', check_same_thread=False)
    cursor = conn.cursor()
    logger.info("Successfully connected to database")
except Exception as e:
    logger.error(f"Failed to connect to database: {e}")
    raise

# Проверка и миграция таблицы accounts
cursor.execute("PRAGMA table_info(accounts)")
columns = [col[1] for col in cursor.fetchall()]
if 'description' not in columns or 'price' not in columns:
    logger.info("Migrating accounts table to include description and price columns")
    cursor.execute('''
    CREATE TABLE accounts_new (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT,
        login TEXT,
        password TEXT,
        level TEXT,
        claimed_by INTEGER DEFAULT NULL,
        claim_date TEXT DEFAULT NULL,
        price INTEGER,
        description TEXT
    )
    ''')
    cursor.execute('''
    INSERT INTO accounts_new (id, category, login, password, level, claimed_by, claim_date)
    SELECT id, category, login, password, level, claimed_by, claim_date FROM accounts
    ''')
    cursor.execute('DROP TABLE accounts')
    cursor.execute('ALTER TABLE accounts_new RENAME TO accounts')
    conn.commit()
    logger.info("Accounts table migration completed")

# Проверка и миграция таблицы user_accounts
cursor.execute("PRAGMA table_info(user_accounts)")
columns = [col[1] for col in cursor.fetchall()]
if 'description' not in columns:
    logger.info("Migrating user_accounts table to include description column")
    cursor.execute('''
    CREATE TABLE user_accounts_new (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        account_id INTEGER,
        category TEXT,
        login TEXT,
        password TEXT,
        level TEXT,
        description TEXT,
        claim_date TEXT
    )
    ''')
    cursor.execute('''
    INSERT INTO user_accounts_new (id, user_id, account_id, category, login, password, level, claim_date)
    SELECT id, user_id, account_id, category, login, password, level, claim_date FROM user_accounts
    ''')
    cursor.execute('DROP TABLE user_accounts')
    cursor.execute('ALTER TABLE user_accounts_new RENAME TO user_accounts')
    conn.commit()
    logger.info("User_accounts table migration completed")

# Создание остальных таблиц (если они еще не существуют)
cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    full_name TEXT,
    referral_id TEXT,
    referred_by TEXT,
    balance INTEGER DEFAULT 0,
    free_account_claimed INTEGER DEFAULT 0,
    referrals_count INTEGER DEFAULT 0,
    is_subscribed INTEGER DEFAULT 0,
    captcha_passed INTEGER DEFAULT 0,
    join_date TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS categories (
    name TEXT PRIMARY KEY,
    display_name TEXT
)
''')

try:
    cursor.execute("INSERT OR IGNORE INTO categories (name, display_name) VALUES (?, ?)",
                   ("Бесплатные", "Бесплатные"))
    cursor.execute("INSERT OR IGNORE INTO categories (name, display_name) VALUES (?, ?)",
                   ("Мир танков", "Мир танков"))
    cursor.execute("INSERT OR IGNORE INTO categories (name, display_name) VALUES (?, ?)",
                   ("Блиц", "Блиц"))
    conn.commit()
    logger.info("Categories initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize categories: {e}")

# Состояния FSM
class AdminStates(StatesGroup):
    waiting_for_category = State()
    waiting_for_new_product_category = State()
    waiting_for_new_product_level = State()
    waiting_for_new_product_price = State()
    waiting_for_new_product_description = State()
    waiting_for_new_product_accounts = State()
    waiting_for_replenish_product = State()
    waiting_for_replenish_accounts = State()
    waiting_for_broadcast = State()
    waiting_for_add_coins = State()
    waiting_for_delete_product = State()

class UserStates(StatesGroup):
    captcha = State()
    selecting_product = State()
    viewing_accounts = State()

# Генерация случайной капчи
def generate_captcha(length=CAPTCHA_LENGTH):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Проверка подписки на канал
async def check_subscription(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=f"@{CHANNEL_ID}", user_id=user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        logger.error(f"Error checking subscription for user {user_id}: {e}")
        return False

# Главное меню
def main_menu_keyboard() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📦 Получить аккаунт")],
            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="📦 Мои аккаунты")],
            [KeyboardButton(text="💰 Заработать слитки")]
        ],
        resize_keyboard=True
    )
    return keyboard

def admin_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.add(
        InlineKeyboardButton(text="📤 Загрузить новый товар", callback_data="upload_product"),
        InlineKeyboardButton(text="🔄 Пополнить товар", callback_data="replenish_product"),
        InlineKeyboardButton(text="🗑️ Удалить товары", callback_data="delete_products"),
        InlineKeyboardButton(text="📊 Статистика", callback_data="stats"),
        InlineKeyboardButton(text="📋 Просмотр товаров", callback_data="view_products"),
        InlineKeyboardButton(text="📢 Сделать рассылку", callback_data="broadcast"),
        InlineKeyboardButton(text="💰 Начислить слитки", callback_data="add_coins"),
        InlineKeyboardButton(text="📝 Категории", callback_data="categories"),
    )
    builder.adjust(2)
    return builder.as_markup()


@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    args = message.text.split()
    referral_id = args[1] if len(args) > 1 else None

    cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = cursor.fetchone()

    if not user:
        username = message.from_user.username
        full_name = message.from_user.full_name
        join_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        referred_by = None
        if referral_id:
            cursor.execute("SELECT user_id, referrals_count FROM users WHERE referral_id = ?", (referral_id,))
            referrer = cursor.fetchone()
            if referrer:
                referred_by, referrals_count = referrer
                cursor.execute(
                    "UPDATE users SET balance = balance + 1, referrals_count = referrals_count + 1 WHERE user_id = ?",
                    (referred_by,)
                )
                # Уведомляем реферера о новом реферале
                try:
                    await bot.send_message(
                        referred_by,
                        f"{INDENT}У вас новый реферал! +1 слиток 🎉\n"
                        f"{INDENT}Теперь у вас <b>{referrals_count + 1}</b> рефералов."
                    )
                    logger.info(f"Sent referral notification to user {referred_by}")
                except Exception as e:
                    logger.error(f"Failed to send referral notification to user {referred_by}: {e}")

        ref_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        cursor.execute(
            "INSERT INTO users (user_id, username, full_name, referral_id, referred_by, join_date) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, username, full_name, ref_id, referred_by, join_date)
        )
        conn.commit()

        captcha = generate_captcha()
        await state.update_data(captcha=captcha)
        await state.set_state(UserStates.captcha)
        await message.answer(f"{INDENT}Пожалуйста, введите следующий код для подтверждения: <b>{captcha}</b>")
    else:
        is_subscribed = await check_subscription(user_id)
        if is_subscribed:
            cursor.execute("UPDATE users SET is_subscribed = 1 WHERE user_id = ?", (user_id,))
            conn.commit()
            await show_welcome_message(message)
        else:
            await message.answer(f"{INDENT}Пожалуйста, подпишитесь на наш канал, чтобы продолжить пользоваться ботом.")
            await prompt_subscription(message)

# Показать приветственное сообщение
async def show_welcome_message(message: types.Message):
    user_id = message.from_user.id
    cursor.execute("SELECT full_name FROM users WHERE user_id = ?", (user_id,))
    user = cursor.fetchone()

    welcome_text = (
        f"{INDENT}Привет, <b>{user[0]}</b> 👋\n"
        f"{INDENT}В этом боте ты можешь получить бесплатные аккаунты, приглашая друзей через реферальную ссылку "
        "или выполняя задания 💰"
    )
    await message.answer(welcome_text, reply_markup=main_menu_keyboard())

# Обработчик ввода капчи
@dp.message(UserStates.captcha)
async def process_captcha(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    if message.text.upper() == user_data['captcha'].upper():
        await state.update_data(captcha_passed=True)
        await state.clear()

        is_subscribed = await check_subscription(message.from_user.id)
        if is_subscribed:
            cursor.execute("UPDATE users SET captcha_passed = 1, is_subscribed = 1 WHERE user_id = ?",
                           (message.from_user.id,))
            conn.commit()
            await show_welcome_message(message)
        else:
            await prompt_subscription(message)
    else:
        captcha = generate_captcha()
        await state.update_data(captcha=captcha)
        await message.answer(f"{INDENT}Для начала введите код. Попробуйте еще раз: <b>{captcha}</b>")

# Запрос подписки на канал
async def prompt_subscription(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Подписаться", url=CHANNEL_URL)],
        [InlineKeyboardButton(text="Я подписался", callback_data="check_subscription")]
    ])
    await message.answer(
        f"{INDENT}📢 Для продолжения работы с ботом необходимо подписаться на наш канал:",
        reply_markup=keyboard
    )

# Обработчик проверки подписки
@dp.callback_query(F.data == "check_subscription")
async def check_subscription_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    is_subscribed = await check_subscription(user_id)
    if is_subscribed:
        cursor.execute("UPDATE users SET is_subscribed = 1 WHERE user_id = ?", (user_id,))
        conn.commit()
        await callback.message.delete()
        # Показываем приветственное сообщение и клавиатуру
        await show_welcome_message(callback.message)
    else:
        await callback.answer("Вы не подписаны на канал. Пожалуйста, подпишитесь и попробуйте снова.", show_alert=True)

# Обработчик кнопки "Получить аккаунт"
@dp.message(F.text == "📦 Получить аккаунт")
async def get_account(message: types.Message):
    user_id = message.from_user.id

    is_subscribed = await check_subscription(user_id)
    if not is_subscribed:
        await prompt_subscription(message)
        return

    cursor.execute("SELECT free_account_claimed FROM users WHERE user_id = ?", (user_id,))
    user = cursor.fetchone()

    if user and user[0] == 0:
        await handle_free_account(message)
    else:
        await show_paid_accounts_menu(message)

# Обработчик бесплатного аккаунта
async def handle_free_account(message: types.Message):
    user_id = message.from_user.id

    cursor.execute(
        "SELECT id, login, password FROM accounts WHERE category = ? AND claimed_by IS NULL LIMIT 1",
        (FREE_ACCOUNT_CATEGORY,)
    )
    account = cursor.fetchone()

    if account:
        account_id, login, password = account
        claim_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            cursor.execute(
                "INSERT INTO user_accounts (user_id, account_id, category, login, password, claim_date) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, account_id, FREE_ACCOUNT_CATEGORY, login, password, claim_date))
            cursor.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
            cursor.execute("UPDATE users SET free_account_claimed = 1 WHERE user_id = ?", (user_id,))
            conn.commit()
            logger.info(f"Free account {account_id} claimed by user {user_id}")
        except Exception as e:
            logger.error(f"Error saving free account for user {user_id}: {e}")
            await message.answer(f"{INDENT}Ошибка при выдаче аккаунта. Попробуйте позже.")
            return

        account_text = (
            f"{INDENT}🔑 Бесплатный аккаунт:\n\n"
            f"{INDENT}👤 Логин: <b>{login}</b>\n"
            f"{INDENT}🔒 Пароль: <b>{password}</b>\n\n"
            f"{INDENT}Спасибо за использование нашего бота!"
        )
        await message.answer(account_text)
    else:
        await message.answer(f"{INDENT}К сожалению, бесплатные аккаунты закончились. Попробуйте позже.")

# Показать меню платных аккаунтов
async def show_paid_accounts_menu(message: types.Message):
    user_id = message.from_user.id

    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    balance = cursor.fetchone()[0]

    cursor.execute("SELECT name, display_name FROM categories WHERE name != ?", (FREE_ACCOUNT_CATEGORY,))
    categories = cursor.fetchall()

    if not categories:
        await message.answer(f"{INDENT}В настоящее время нет доступных категорий товаров.")
        return

    text = f"{INDENT}💰 У вас <b>{balance}</b> слитков\n\n{INDENT}Выберите категорию:"

    builder = InlineKeyboardBuilder()
    for category in categories:
        builder.add(InlineKeyboardButton(
            text=category[1],
            callback_data=f"category_{category[0]}"
        ))

    builder.adjust(1)
    await message.answer(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("category_"))
async def process_category_selection(callback: types.CallbackQuery, state: FSMContext):
    category = callback.data.split("_")[1]
    user_id = callback.from_user.id

    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    balance = cursor.fetchone()[0]
    cursor.execute("SELECT display_name FROM categories WHERE name = ?", (category,))
    display_name = cursor.fetchone()[0]

    # Получаем все уникальные товары в категории, игнорируя level
    cursor.execute(
        """
        SELECT TRIM(LOWER(description)) as description, COALESCE(price, 0) as price, NULL as level, COUNT(*) as count
        FROM accounts
        WHERE category = ? AND claimed_by IS NULL
        GROUP BY TRIM(LOWER(description)), price
        """,
        (category,)
    )
    products = cursor.fetchall()

    # Логируем товары для отладки
    for product in products:
        description, price, level, count = product
        logger.info(f"Product in category {category}: description={description}, price={price}, level={level}, count={count}")

    text = f"{INDENT}💰 У вас <b>{balance}</b> слитков\n\n{INDENT}Выберите товар в <b>{display_name}</b>:"
    builder = InlineKeyboardBuilder()

    # Сохраняем список продуктов в состоянии для пагинации
    product_list = []
    for product in products:
        description, price, level, count = product
        description = description if description else "Без названия"
        level = level if level else "Не указан"
        product_list.append({
            "category": category,
            "description": description,
            "price": price,
            "level": level,
            "count": count
        })

    if not product_list:
        text = f"{INDENT}💰 У вас <b>{balance}</b> слитков\n\n{INDENT}В категории <b>{display_name}</b> нет доступных товаров."
        logger.warning(f"No available products found in category: {category}")
    else:
        await state.update_data(category=category, product_list=product_list, page=0)
        await state.set_state(UserStates.selecting_product)
        await show_products_page(callback.message, None, user_id, state)

    builder.add(InlineKeyboardButton(
        text="⬅️ Назад к категориям",
        callback_data="back_to_categories"
    ))
    builder.adjust(1)

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# Функция для отображения страницы товаров
async def show_products_page(message: types.Message, callback: types.CallbackQuery, user_id: int, state: FSMContext):
    state_data = await state.get_data()
    product_list = state_data.get("product_list", [])
    page = state_data.get("page", 0)
    category = state_data.get("category")
    cursor.execute("SELECT display_name FROM categories WHERE name = ?", (category,))
    display_name = cursor.fetchone()[0]
    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    balance_result = cursor.fetchone()

    if balance_result is None:
        logger.error(f"No user found with user_id {user_id}")
        text = f"{INDENT}Ошибка: пользователь не найден. Пожалуйста, перезапустите бота (/start)."
        if callback:
            await callback.message.edit_text(text)
        else:
            await message.answer(text)
        return

    balance = balance_result[0]

    total_products = len(product_list)
    total_pages = math.ceil(total_products / PRODUCTS_PER_PAGE)
    start_idx = page * PRODUCTS_PER_PAGE
    end_idx = min(start_idx + PRODUCTS_PER_PAGE, total_products)
    current_products = product_list[start_idx:end_idx]

    text = f"{INDENT}💰 У вас <b>{balance}</b> слитков\n\n{INDENT}Выберите товар в <b>{display_name}</b> (Страница {page + 1}/{total_pages}):\n\n"
    builder = InlineKeyboardBuilder()

    for idx, product in enumerate(current_products, start=start_idx + 1):
        description, price, level, count = product["description"], product["price"], product["level"], product["count"]
        builder.add(InlineKeyboardButton(
            text=f"{description} - {price} слитков (в наличии: {count})",
            callback_data=f"buy_product_{idx - 1}"
        ))

    # Кнопки навигации
    if page > 0:
        builder.add(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"product_page_{page - 1}"))
    if page < total_pages - 1:
        builder.add(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"product_page_{page + 1}"))

    builder.adjust(1)
    if callback:
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
    else:
        await message.answer(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("product_page_"))
async def process_product_page(callback: types.CallbackQuery, state: FSMContext):
    page = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    await state.update_data(page=page)
    await show_products_page(None, callback, user_id, state)

# Обработчик покупки товара
@dp.callback_query(F.data.startswith("buy_product_"))
async def process_account_purchase(callback: types.CallbackQuery, state: FSMContext):
    index = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    state_data = await state.get_data()
    product_list = state_data.get("product_list", [])
    category = state_data.get("category")

    if index < 0 or index >= len(product_list):
        await callback.answer("Ошибка: товар не найден.", show_alert=True)
        return

    product = product_list[index]
    description = product["description"]
    price = product["price"]

    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    balance = cursor.fetchone()[0]

    if balance < price:
        await callback.answer("Недостаточно слитков для покупки! Заработайте слитки, приглашая друзей.",
                              show_alert=True)
        return

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM accounts
        WHERE category = ?
        AND TRIM(LOWER(description)) = TRIM(LOWER(?))
        AND claimed_by IS NULL
        """,
        (category, description if description != "Без названия" else None)
    )
    available_accounts = cursor.fetchone()[0]
    logger.info(f"Available accounts for purchase in category {category}, description {description}: {available_accounts}")

    if available_accounts == 0:
        await callback.answer("К сожалению, товары в этой категории закончились.", show_alert=True)
        return

    try:
        cursor.execute(
            """
            SELECT id, login, password, level
            FROM accounts
            WHERE category = ?
            AND TRIM(LOWER(description)) = TRIM(LOWER(?))
            AND claimed_by IS NULL
            ORDER BY RANDOM()
            LIMIT 1
            """,
            (category, description if description != "Без названия" else None)
        )
        account = cursor.fetchone()

        if account:
            account_id, login, password, account_level = account
            claim_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            try:
                cursor.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (price, user_id))
                cursor.execute(
                    "INSERT INTO user_accounts (user_id, account_id, category, login, password, level, description, claim_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (user_id, account_id, category, login, password, account_level or "", description, claim_date))
                cursor.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
                conn.commit()
                logger.info(f"Account {account_id} purchased by user {user_id} for {price} coins")
            except Exception as e:
                logger.error(f"Error processing purchase for user {user_id}, account {account_id}: {e}")
                conn.rollback()
                await callback.answer("Ошибка при покупке товара. Попробуйте позже.", show_alert=True)
                return

            account_text = (
                f"{INDENT}🔑 Купленный товар (<b>{price}</b> слитков):\n\n"
                f"{INDENT}📜 Описание: <b>{description}</b>\n"
                f"{INDENT}👤 Логин: <b>{login}</b>\n"
                f"{INDENT}🔒 Пароль: <b>{password}</b>\n"
                f"{INDENT}📊 Уровень: {account_level or 'Не указан'}\n\n"
                f"{INDENT}Спасибо за покупку!"
            )
            await callback.message.answer(account_text)
            await callback.message.delete()
        else:
            logger.warning(f"No account found for category {category}, description {description}")
            await callback.answer(
                f"К сожалению, товары '{description}' закончились. Попробуйте другой вариант или обратитесь к админу.",
                show_alert=True
            )
    except Exception as e:
        logger.error(f"Error querying account for purchase in category {category}, description {description}: {e}")
        await callback.answer("Ошибка при выборе товара. Попробуйте позже.", show_alert=True)

# Обработчик возврата к категориям
@dp.callback_query(F.data == "back_to_categories")
async def back_to_categories(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id

    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    balance = cursor.fetchone()[0]

    cursor.execute("SELECT name, display_name FROM categories WHERE name != ?", (FREE_ACCOUNT_CATEGORY,))
    categories = cursor.fetchall()

    if not categories:
        await callback.message.edit_text(f"{INDENT}В настоящее время нет доступных категорий товаров.")
        return

    text = f"{INDENT}💰 У вас <b>{balance}</b> слитков\n\n{INDENT}Выберите категорию:"

    builder = InlineKeyboardBuilder()
    for category in categories:
        builder.add(InlineKeyboardButton(
            text=category[1],
            callback_data=f"category_{category[0]}"
        ))

    builder.adjust(1)
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# Обработчик кнопки "Профиль"
@dp.message(F.text == "👤 Профиль")
async def show_profile(message: types.Message):
    user_id = message.from_user.id

    try:
        cursor.execute(
            "SELECT full_name, balance, referrals_count, referral_id FROM users WHERE user_id = ?",
            (user_id,))
        user = cursor.fetchone()

        if user:
            full_name, balance, referrals_count, referral_id = user
            profile_text = (
                f"{INDENT}👤 Ваш профиль:\n\n"
                f"{INDENT}💰 У вас <b>{balance}</b> слитков\n"
                f"{INDENT}📈 Вы пригласили: <b>{referrals_count}</b> друзей\n"
                f"{INDENT}🔗 Ваша реферальная ссылка:\n"
                f"{INDENT}https://t.me/{(await bot.get_me()).username}?start={referral_id}"
            )
            await message.answer(profile_text)
        else:
            await message.answer(f"{INDENT}Пользователь не найден. Попробуйте перезапустить бота (/start).")
    except Exception as e:
        logger.error(f"Error fetching profile for user {user_id}: {e}")
        await message.answer(f"{INDENT}Ошибка при загрузке профиля. Попробуйте позже.")

# Функция для отображения страницы аккаунтов
async def show_accounts_page(message: types.Message, callback: types.CallbackQuery, accounts: List, page: int, state: FSMContext):
    total_accounts = len(accounts)
    total_pages = math.ceil(total_accounts / PRODUCTS_PER_PAGE)
    start_idx = page * PRODUCTS_PER_PAGE
    end_idx = min(start_idx + PRODUCTS_PER_PAGE, total_accounts)
    current_accounts = accounts[start_idx:end_idx]

    text = f"{INDENT}📦 Ваши товары (Страница {page + 1}/{total_pages}):\n\n"
    for idx, account in enumerate(current_accounts, start=start_idx + 1):
        category, login, password, level, description, claim_date = account
        description = description if description is not None else "Без названия"
        text += (
            f"{INDENT}{idx}. {category or 'Не указана'}\n"
            f"{INDENT}📜 Описание: <b>{description}</b>\n"
            f"{INDENT}👤 Логин: <b>{login}</b>\n"
            f"{INDENT}🔒 Пароль: <b>{password}</b>\n"
            f"{INDENT}📊 Уровень: {level or 'Не указан'}\n"
            f"{INDENT}📅 Получен: {claim_date[:10]}\n\n"
        )

    # Создаём кнопки навигации
    builder = InlineKeyboardBuilder()
    if page > 0:
        builder.add(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"accounts_page_{page - 1}"))
    if page < total_pages - 1:
        builder.add(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"accounts_page_{page + 1}"))
    builder.adjust(2)

    if callback:
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
    else:
        await message.answer(text, reply_markup=builder.as_markup())

# Обработчик кнопки "Мои аккаунты"
@dp.message(F.text == "📦 Мои аккаунты")
async def show_my_accounts(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    try:
        cursor.execute('''
            SELECT category, login, password, level, description, claim_date 
            FROM user_accounts
            WHERE user_id = ?
            ORDER BY claim_date DESC
        ''', (user_id,))
        accounts = cursor.fetchall()

        if not accounts:
            await message.answer(f"{INDENT}У вас пока нет купленных товаров.")
            return

        # Сохраняем список аккаунтов в состоянии
        await state.update_data(accounts=accounts, page=0)
        await state.set_state(UserStates.viewing_accounts)

        # Показываем первую страницу
        await show_accounts_page(message, None, accounts, 0, state)

    except Exception as e:
        logger.error(f"Error fetching accounts for user {user_id}: {e}")
        await message.answer(f"{INDENT}Ошибка при загрузке товаров. Попробуйте позже.")

# Обработчик перелистывания страниц аккаунтов
@dp.callback_query(F.data.startswith("accounts_page_"))
async def process_accounts_page(callback: types.CallbackQuery, state: FSMContext):
    page = int(callback.data.split("_")[2])
    state_data = await state.get_data()
    accounts = state_data.get("accounts", [])

    if not accounts:
        await callback.message.edit_text(f"{INDENT}Ошибка: данные о товарах потеряны. Попробуйте снова.")
        await state.clear()
        return

    await state.update_data(page=page)
    await show_accounts_page(None, callback, accounts, page, state)

# Обработчик кнопки "Заработать слитки"
@dp.message(F.text == "💰 Заработать слитки")
async def earn_coins(message: types.Message):
    user_id = message.from_user.id

    try:
        cursor.execute(
            "SELECT balance, referral_id, referrals_count FROM users WHERE user_id = ?",
            (user_id,))
        user = cursor.fetchone()

        if user:
            balance, referral_id, referrals_count = user
            earn_text = (
                f"{INDENT}💰 У вас <b>{balance}</b> слитков\n\n"
                f"{INDENT}💰 Заработать слитки можно так:\n"
                f"{INDENT}Приглашай друзей по своей ссылке:\n"
                f"{INDENT}https://t.me/{(await bot.get_me()).username}?start={referral_id}\n\n"
                f"{INDENT}За каждого приглашенного друга вы получите <b>1</b> слиток!"
            )
            await message.answer(earn_text)
        else:
            await message.answer(f"{INDENT}Пользователь не найден. Попробуйте перезапустить бота (/start).")
    except Exception as e:
        logger.error(f"Error fetching earn coins info for user {user_id}: {e}")
        await message.answer(f"{INDENT}Ошибка при загрузке информации. Попробуйте позже.")

# Админ-команды
@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer(f"{INDENT}У вас нет прав администратора.")
        return

    await message.answer(f"{INDENT}Админ-панель:", reply_markup=admin_keyboard())

# Обработчик загрузки нового товара
@dp.callback_query(F.data == "upload_product")
async def upload_product(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return

    cursor.execute("SELECT name, display_name FROM categories")
    categories = cursor.fetchall()

    if not categories:
        await callback.message.answer(f"{INDENT}Сначала создайте категории через админ-панель.")
        return

    builder = InlineKeyboardBuilder()
    for category in categories:
        builder.add(InlineKeyboardButton(
            text=category[1],
            callback_data=f"new_product_category_{category[0]}"
        ))
    builder.adjust(2)

    await state.set_state(AdminStates.waiting_for_new_product_category)
    await callback.message.answer(
        f"{INDENT}Выберите категорию для нового товара:",
        reply_markup=builder.as_markup()
    )

# Обработчик выбора категории для нового товара
@dp.callback_query(F.data.startswith("new_product_category_"))
async def select_new_product_category(callback: types.CallbackQuery, state: FSMContext):
    category = callback.data.split("_")[3]
    await state.update_data(category=category)

    await state.set_state(AdminStates.waiting_for_new_product_level)
    await callback.message.answer(
        f"{INDENT}Введите уровень товара для категории <b>{category}</b> (например, 0, 10, 8-10, или 'random'):\n"
        f"{INDENT}Отправьте /cancel для отмены."
    )

# Обработчик ввода уровня для нового товара
@dp.message(AdminStates.waiting_for_new_product_level)
async def process_new_product_level(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer(f"{INDENT}Добавление товара отменено.")
        return

    level_input = message.text.strip().lower()
    if level_input != "random" and "-" not in level_input:
        try:
            level_int = int(level_input)
            if level_int < 0:
                await message.answer(f"{INDENT}Ошибка: уровень должен быть положительным числом.")
                return
        except ValueError:
            await message.answer(
                f"{INDENT}Ошибка: уровень должен быть числом, диапазоном (например, 1-3) или 'random'."
            )
            return

    await state.update_data(level=level_input)
    await state.set_state(AdminStates.waiting_for_new_product_price)
    await message.answer(
        f"{INDENT}Введите цену товара в слитках (целое число, например, 50).\n"
        f"{INDENT}Отправьте /cancel для отмены."
    )

# Обработчик ввода цены товара
@dp.message(AdminStates.waiting_for_new_product_price)
async def process_new_product_price(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer(f"{INDENT}Добавление товара отменено.")
        return

    try:
        price = int(message.text.strip())
        if price < 0:
            await message.answer(f"{INDENT}Цена должна быть неотрицательным числом.")
            return
    except ValueError:
        await message.answer(f"{INDENT}Пожалуйста, введите корректное число для цены.")
        return

    await state.update_data(price=price)

    await state.set_state(AdminStates.waiting_for_new_product_description)
    await message.answer(
        f"{INDENT}Введите название товара (например, 'Аккаунт 10 уровня'):\n"
        f"{INDENT}Отправьте /cancel для отмены."
    )

# Обработчик ввода названия товара
@dp.message(AdminStates.waiting_for_new_product_description)
async def process_new_product_description(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer(f"{INDENT}Добавление товара отменено.")
        return

    description = message.text.strip()
    await state.update_data(description=description)

    await state.set_state(AdminStates.waiting_for_new_product_accounts)
    await message.answer(
        f"{INDENT}Отправьте аккаунты для товара(если рандом или лвл-лвл то добавлять"
        f" к концу :и тут лвл акка. пример: логин:пароль:3 в формате:\n"
        f"{INDENT}логин:пароль\n"
        f"{INDENT}логин2:пароль2\n"
        f"{INDENT}и т.д.\n\n"
        f"{INDENT}Отправьте /cancel для отмены."
    )

# Обработчик ввода аккаунтов для нового товара
@dp.message(AdminStates.waiting_for_new_product_accounts)
async def process_new_product_accounts(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer(f"{INDENT}Добавление товара отменено.")
        return

    data = await state.get_data()
    category = data.get('category')
    level_input = data.get('level')
    price = data.get('price')
    description = ' '.join(data.get('description').strip().split())  # Нормализация множественных пробелов

    accounts_text = message.text.strip()
    if not accounts_text:
        await message.answer(f"{INDENT}Ошибка: отправлен пустой список аккаунтов.")
        await state.clear()
        return

    accounts = accounts_text.split('\n')
    added = 0
    errors = []

    # Проверяем, существует ли товар с такими параметрами
    cursor.execute(
        "SELECT COUNT(*) FROM accounts WHERE category = ? AND TRIM(LOWER(description)) = TRIM(LOWER(?)) AND price = ?",
        (category, description, price)
    )
    existing_count = cursor.fetchone()[0]

    for account in accounts:
        account = account.strip()
        if not account:
            continue

        # Разделяем строку на части (логин:пароль:уровень или логин:пароль)
        parts = account.split(':')
        if len(parts) < 2:
            errors.append(f"Неверный формат: {account}. Ожидается логин:пароль[:уровень]")
            continue

        login = parts[0].strip()
        password = parts[1].strip()
        account_level = None

        # Проверяем, указан ли уровень
        if len(parts) > 2:
            level_part = parts[2].strip()
            if '-' in level_part:  # Диапазон, например, 1-3
                try:
                    start, end = map(int, level_part.split('-'))
                    if start < 0 or end < 0 or start > end:
                        errors.append(f"Некорректный диапазон уровня: {level_part}")
                        continue
                    account_level = str(random.randint(start, end))
                    logger.info(f"Level for {login} set from range {level_part}: {account_level}")
                except ValueError:
                    errors.append(f"Некорректный формат диапазона уровня: {level_part}")
                    continue
            else:  # Конкретное число, например, 5
                try:
                    account_level = str(int(level_part))
                    logger.info(f"Level for {login} set explicitly: {account_level}")
                except ValueError:
                    errors.append(f"Некорректное значение уровня: {level_part}")
                    continue
        else:
            # Если уровень не указан, используем level_input
            if level_input == "random":
                account_level = str(random.randint(1, 10))
                logger.info(f"Level for {login} set randomly (1-10): {account_level}")
            elif "-" in level_input:
                start, end = map(int, level_input.split("-"))
                account_level = str(random.randint(start, end))
                logger.info(f"Level for {login} set from range {level_input}: {account_level}")
            else:
                account_level = level_input
                logger.info(f"Level for {login} set from specified level: {account_level}")

        if not login or not password:
            errors.append(f"Пустой логин или пароль: {account}")
            continue

        # Логируем данные перед добавлением
        logger.info(f"Adding account: category={category}, description={description}, price={price}, level={account_level}, login={login}")

        try:
            cursor.execute(
                "INSERT INTO accounts (category, login, password, level, price, description) VALUES (?, ?, ?, ?, ?, ?)",
                (category, login, password, account_level, price, description)
            )
            added += 1
        except Exception as e:
            errors.append(f"Ошибка при добавлении {account}: {str(e)}")

    conn.commit()

    response = f"{INDENT}Успешно добавлено <b>{added}</b> аккаунтов для товара <b>{description}</b> в категорию <b>{category}</b>."
    if existing_count > 0:
        response += f"\n{INDENT}Внимание: товар с таким описанием и ценой уже существует (в наличии: {existing_count})."
    if errors:
        response += f"\n\n{INDENT}Ошибки при добавлении:\n"
        for error in errors:
            response += f"{INDENT}- {error}\n"

    cursor.execute(
        "SELECT id, login, password, level FROM accounts WHERE category = ? AND description = ? ORDER BY id DESC LIMIT ?",
        (category, description, added)
    )
    accounts = cursor.fetchall()

    text = f"{INDENT}Добавленные аккаунты:\n\n"
    for account in accounts:
        account_id, login, password, level = account
        text += (
            f"{INDENT}ID: {account_id}\n"
            f"{INDENT}👤 Логин: <b>{login}</b>\n"
            f"{INDENT}🔒 Пароль: <b>{password}</b>\n"
            f"{INDENT}📊 Уровень: {level or 'Не указан'}\n\n"
        )

    if len(text) > 4096:
        parts = []
        while len(text) > 4096:
            part = text[:4096]
            last_newline = part.rfind('\n')
            if last_newline == -1:
                last_newline = 4096
            parts.append(text[:last_newline])
            text = text[last_newline + 1:]
        parts.append(text)
        await message.answer(response)
        for part in parts:
            await message.answer(part)
    else:
        await message.answer(response)
        await message.answer(text)

    await state.clear()

@dp.callback_query(F.data == "replenish_product")
async def replenish_product(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return

    cursor.execute("SELECT name, display_name FROM categories")
    categories = cursor.fetchall()

    if not categories:
        await callback.message.answer(f"{INDENT}Нет доступных категорий.")
        return

    builder = InlineKeyboardBuilder()
    for category in categories:
        builder.add(InlineKeyboardButton(
            text=category[1],
            callback_data=f"replenish_category_{category[0]}"
        ))
    builder.adjust(2)

    await callback.message.answer(
        f"{INDENT}Выберите категорию для пополнения товаров:",
        reply_markup=builder.as_markup()
    )

# Обработчик выбора категории для пополнения товара
@dp.callback_query(F.data.startswith("replenish_category_"))
async def select_replenish_category(callback: types.CallbackQuery, state: FSMContext):
    category = callback.data.split("_")[2]
    await state.update_data(category=category)

    cursor.execute(
        """
        SELECT TRIM(LOWER(description)) as description, COALESCE(price, 0) as price, NULL as level, COUNT(*) as count
        FROM accounts
        WHERE category = ? AND claimed_by IS NULL
        GROUP BY TRIM(LOWER(description)), price
        """,
        (category,)
    )
    products = cursor.fetchall()

    if not products:
        await callback.message.answer(
            f"{INDENT}В категории <b>{category}</b> нет доступных товаров для пополнения.")
        await state.clear()
        return

    builder = InlineKeyboardBuilder()
    product_list = []
    for product in products:
        description, price, level, count = product
        description = description if description else "Без названия"
        level = level if level else "Не указан"
        product_list.append({"category": category, "description": description, "price": price, "level": level, "count": count})
        index = len(product_list) - 1
        builder.add(InlineKeyboardButton(
            text=f"{description} - {price} слитков (в наличии: {count})",
            callback_data=f"replenish_product_{index}"
        ))

    builder.add(InlineKeyboardButton(
        text="⬅️ Назад",
        callback_data="replenish_product"
    ))
    builder.adjust(1)

    await state.update_data(product_list=product_list)
    await state.set_state(AdminStates.waiting_for_replenish_product)
    await callback.message.edit_text(
        f"{INDENT}Выберите товар для пополнения в категории <b>{category}</b>:",
        reply_markup=builder.as_markup()
    )

# Обработчик выбора товара для пополнения
@dp.callback_query(F.data.startswith("replenish_product_"))
async def select_replenish_product(callback: types.CallbackQuery, state: FSMContext):
    index = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    if user_id not in ADMIN_IDS:
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return

    state_data = await state.get_data()
    product_list = state_data.get("product_list", [])

    if index < 0 or index >= len(product_list):
        await callback.answer("Ошибка: товар не найден.", show_alert=True)
        return

    product = product_list[index]
    description = product["description"]
    await state.update_data(description=description)

    await state.set_state(AdminStates.waiting_for_replenish_accounts)
    await callback.message.edit_text(
        f"{INDENT}Отправьте аккаунты для пополнения товара <b>{description}</b> в формате:\n"
        f"{INDENT}логин:пароль\n"
        f"{INDENT}логин2:пароль2\n"
        f"{INDENT}и т.д.\n\n"
        f"{INDENT}Отправьте /cancel для отмены."
    )

# Обработчик ввода аккаунтов для пополнения товара
@dp.message(AdminStates.waiting_for_replenish_accounts)
async def process_replenish_accounts(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer(f"{INDENT}Пополнение товара отменено.")
        return

    data = await state.get_data()
    category = data.get('category')
    description = ' '.join(data.get('description').strip().split())  # Нормализация множественных пробелов

    cursor.execute(
        """
        SELECT COALESCE(price, 0) as price, TRIM(LOWER(level)) as level
        FROM accounts
        WHERE category = ? AND TRIM(LOWER(description)) = TRIM(LOWER(?))
        LIMIT 1
        """,
        (category, description if description != "Без названия" else None)
    )
    product = cursor.fetchone()
    if not product:
        await message.answer(f"{INDENT}Ошибка: товар не найден.")
        await state.clear()
        return

    price, default_level = product

    accounts_text = message.text.strip()
    if not accounts_text:
        await message.answer(f"{INDENT}Ошибка: отправлен пустой список аккаунтов.")
        await state.clear()
        return

    accounts = accounts_text.split('\n')
    added = 0
    errors = []

    for account in accounts:
        account = account.strip()
        if not account:
            continue

        # Разделяем строку на части (логин:пароль:уровень или логин:пароль)
        parts = account.split(':')
        if len(parts) < 2:
            errors.append(f"Неверный формат: {account}. Ожидается логин:пароль[:уровень]")
            continue

        login = parts[0].strip()
        password = parts[1].strip()
        account_level = None

        # Проверяем, указан ли уровень
        if len(parts) > 2:
            level_part = parts[2].strip()
            if '-' in level_part:  # Диапазон, например, 1-3
                try:
                    start, end = map(int, level_part.split('-'))
                    if start < 0 or end < 0 or start > end:
                        errors.append(f"Некорректный диапазон уровня: {level_part}")
                        continue
                    account_level = str(random.randint(start, end))
                    logger.info(f"Level for {login} set from range {level_part}: {account_level}")
                except ValueError:
                    errors.append(f"Некорректный формат диапазона уровня: {level_part}")
                    continue
            else:  # Конкретное число, например, 5
                try:
                    account_level = str(int(level_part))
                    logger.info(f"Level for {login} set explicitly: {account_level}")
                except ValueError:
                    errors.append(f"Некорректное значение уровня: {level_part}")
                    continue
        else:
            # Если уровень не указан, используем default_level из существующего товара
            account_level = default_level
            logger.info(f"Level for {login} set from default level: {account_level}")

        if not login or not password:
            errors.append(f"Пустой логин или пароль: {account}")
            continue

        # Логируем данные перед добавлением
        logger.info(f"Replenishing account: category={category}, description={description}, price={price}, level={account_level}, login={login}")

        try:
            cursor.execute(
                "INSERT INTO accounts (category, login, password, level, price, description) VALUES (?, ?, ?, ?, ?, ?)",
                (category, login, password, account_level, price, description)
            )
            added += 1
        except Exception as e:
            errors.append(f"Ошибка при добавлении {account}: {str(e)}")

    conn.commit()

    response = f"{INDENT}Успешно добавлено <b>{added}</b> аккаунтов для товара <b>{description}</b> в категорию <b>{category}</b>."
    if errors:
        response += f"\n\n{INDENT}Ошибки при добавлении:\n"
        for error in errors:
            response += f"{INDENT}- {error}\n"

    cursor.execute(
        "SELECT id, login, password, level FROM accounts WHERE category = ? AND description = ? ORDER BY id DESC LIMIT ?",
        (category, description, added)
    )
    accounts = cursor.fetchall()

    text = f"{INDENT}Добавленные аккаунты:\n\n"
    for account in accounts:
        account_id, login, password, level = account
        text += (
            f"{INDENT}ID: {account_id}\n"
            f"{INDENT}👤 Логин: <b>{login}</b>\n"
            f"{INDENT}🔒 Пароль: <b>{password}</b>\n"
            f"{INDENT}📊 Уровень: {level or 'Не указан'}\n\n"
        )

    if len(text) > 4096:
        parts = []
        while len(text) > 4096:
            part = text[:4096]
            last_newline = part.rfind('\n')
            if last_newline == -1:
                last_newline = 4096
            parts.append(text[:last_newline])
            text = text[last_newline + 1:]
        parts.append(text)
        await message.answer(response)
        for part in parts:
            await message.answer(part)
    else:
        await message.answer(response)
        await message.answer(text)

    await state.clear()

@dp.callback_query(F.data == "view_products")
async def view_products(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return

    cursor.execute("SELECT name, display_name FROM categories")
    categories = cursor.fetchall()

    if not categories:
        await callback.message.answer(f"{INDENT}Нет доступных категорий.")
        return

    builder = InlineKeyboardBuilder()
    for category in categories:
        builder.add(InlineKeyboardButton(
            text=category[1],
            callback_data=f"view_category_products_{category[0]}"
        ))
    builder.adjust(2)

    await callback.message.edit_text(
        f"{INDENT}Выберите категорию для просмотра товаров:",
        reply_markup=builder.as_markup()
    )

# Обработчик просмотра товаров в категории
@dp.callback_query(F.data.startswith("view_category_products_"))
async def view_category_products(callback: types.CallbackQuery, state: FSMContext):
    category = callback.data.split("_")[3]

    cursor.execute(
        """
        SELECT TRIM(LOWER(description)) as description, COALESCE(price, 0) as price, NULL as level, COUNT(*) as count
        FROM accounts
        WHERE category = ? AND claimed_by IS NULL
        GROUP BY TRIM(LOWER(description)), price
        """,
        (category,)
    )
    products = cursor.fetchall()

    if not products:
        await callback.message.edit_text(f"{INDENT}В категории <b>{category}</b> нет товаров.")
        return

    text = f"{INDENT}Товары в категории <b>{category}</b>:\n\n"
    for idx, product in enumerate(products, 1):
        description, price, level, count = product
        description = description if description else "Без названия"
        level = level if level else "Не указан"
        text += (
            f"{INDENT}{idx}. Описание: <b>{description}</b>\n"
            f"{INDENT}💰 Цена: {price} слитков\n"
            f"{INDENT}📊 Уровень: {level}\n"
            f"{INDENT}📦 Количество: {count}\n\n"
        )

    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(
        text="⬅️ Назад к категориям",
        callback_data="view_products"
    ))
    builder.adjust(1)

    if len(text) > 4096:
        parts = []
        while len(text) > 4096:
            part = text[:4096]
            last_newline = part.rfind('\n')
            if last_newline == -1:
                last_newline = 4096
            parts.append(text[:last_newline])
            text = text[last_newline + 1:]
        parts.append(text)
        await callback.message.edit_text(parts[0], reply_markup=builder.as_markup())
        for part in parts[1:]:
            await callback.message.answer(part)
    else:
        await callback.message.edit_text(text, reply_markup=builder.as_markup())

# Обработчик удаления товаров
@dp.callback_query(F.data == "delete_products")
async def delete_products(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return

    cursor.execute("SELECT name, display_name FROM categories")
    categories = cursor.fetchall()

    if not categories:
        await callback.message.answer(f"{INDENT}Нет доступных категорий для удаления товаров.")
        return

    builder = InlineKeyboardBuilder()
    for category in categories:
        builder.add(InlineKeyboardButton(
            text=category[1],
            callback_data=f"select_delete_product_category_{category[0]}"
        ))
    builder.adjust(2)

    await callback.message.answer(
        f"{INDENT}Выберите категорию для удаления товаров:",
        reply_markup=builder.as_markup()
    )

# Обработчик выбора категории для удаления товара
@dp.callback_query(F.data.startswith("select_delete_product_category_"))
async def select_delete_product_category(callback: types.CallbackQuery, state: FSMContext):
    category = callback.data.split("_")[4]
    await state.update_data(category=category)

    cursor.execute(
        """
        SELECT TRIM(LOWER(description)) as description, COALESCE(price, 0) as price, NULL as level, COUNT(*) as count
        FROM accounts
        WHERE category = ? AND claimed_by IS NULL
        GROUP BY TRIM(LOWER(description)), price
        """,
        (category,)
    )
    products = cursor.fetchall()

    if not products:
        await callback.message.answer(
            f"{INDENT}В категории <b>{category}</b> нет доступных товаров для удаления.")
        await state.clear()
        return

    builder = InlineKeyboardBuilder()
    product_list = []
    for product in products:
        description, price, level, count = product
        description = description if description else "Без названия"
        level = level if level else "Не указан"
        product_list.append({"category": category, "description": description, "price": price, "level": level, "count": count})
        index = len(product_list) - 1
        builder.add(InlineKeyboardButton(
            text=f"{description} (в наличии: {count})",
            callback_data=f"delete_product_{index}"
        ))

    builder.add(InlineKeyboardButton(
        text="⬅️ Назад",
        callback_data="delete_products"
    ))
    builder.adjust(1)

    await state.update_data(product_list=product_list)
    await state.set_state(AdminStates.waiting_for_delete_product)
    await callback.message.edit_text(
        f"{INDENT}Выберите товар для удаления в категории <b>{category}</b>:",
        reply_markup=builder.as_markup()
    )

# Обработчик удаления конкретного товара
@dp.callback_query(F.data.startswith("delete_product_"))
async def process_product_deletion(callback: types.CallbackQuery, state: FSMContext):
    index = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    if user_id not in ADMIN_IDS:
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return

    state_data = await state.get_data()
    product_list = state_data.get("product_list", [])
    category = state_data.get("category")

    if index < 0 or index >= len(product_list):
        await callback.answer("Ошибка: товар не найден.", show_alert=True)
        return

    product = product_list[index]
    description = product["description"]

    try:
        cursor.execute(
            """
            DELETE FROM accounts
            WHERE category = ?
            AND TRIM(LOWER(description)) = TRIM(LOWER(?))
            """,
            (category, description if description != "Без названия" else None)
        )
        conn.commit()
        logger.info(f"Successfully deleted product with description {description} in category {category}")
    except Exception as e:
        await callback.answer(f"Ошибка при удалении товара: {str(e)}", show_alert=True)
        logger.error(f"Error deleting product with description {description}: {e}")
        return

    await callback.message.edit_text(
        f"{INDENT}Товар <b>{description}</b> успешно удален из категории <b>{category}</b>."
    )
    await state.clear()

# Обработчик статистики
@dp.callback_query(F.data == "stats")
async def show_stats(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return

    try:
        # Общее количество пользователей
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()[0]

        # Общее количество доступных товаров
        cursor.execute("SELECT COUNT(*) FROM accounts WHERE claimed_by IS NULL")
        total_products = cursor.fetchone()[0]

        # Общее количество купленных аккаунтов
        cursor.execute("SELECT COUNT(*) FROM user_accounts")
        total_purchased = cursor.fetchone()[0]

        # Общее количество рефералов
        cursor.execute("SELECT SUM(referrals_count) FROM users")
        total_referrals = cursor.fetchone()[0] or 0

        stats_text = (
            f"{INDENT}📊 Статистика бота:\n\n"
            f"{INDENT}👥 Всего пользователей: <b>{total_users}</b>\n"
            f"{INDENT}📦 Доступных товаров: <b>{total_products}</b>\n"
            f"{INDENT}🛒 Куплено аккаунтов: <b>{total_purchased}</b>\n"
            f"{INDENT}📈 Всего рефералов: <b>{total_referrals}</b>"
        )
        await callback.message.edit_text(stats_text)
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        await callback.message.edit_text(f"{INDENT}Ошибка при загрузке статистики: {str(e)}")

# Обработчик начала начисления слитков
@dp.callback_query(F.data == "add_coins")
async def start_add_coins(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return

    await state.set_state(AdminStates.waiting_for_add_coins)
    await callback.message.answer(
        f"{INDENT}Введите данные для начисления слитков в формате:\n"
        f"{INDENT}user_id:количество (например, 12345:10)\n"
        f"{INDENT}Отправьте /cancel для отмены."
    )

# Обработчик ввода данных для начисления слитков
@dp.message(AdminStates.waiting_for_add_coins)
async def process_add_coins(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer(f"{INDENT}Начисление слитков отменено.")
        return

    try:
        user_id, coins = map(int, message.text.strip().split(':'))
        cursor.execute("SELECT user_id, balance FROM users WHERE user_id = ?", (user_id,))
        user = cursor.fetchone()

        if not user:
            await message.answer(f"{INDENT}Пользователь с ID <b>{user_id}</b> не найден.")
            return

        cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (coins, user_id))
        conn.commit()

        # Уведомляем пользователя о начислении слитков
        try:
            new_balance = user[1] + coins
            await bot.send_message(
                user_id,
                f"{INDENT}💰 Вам начислено <b>{coins}</b> слитков администратором!\n"
                f"{INDENT}Ваш новый баланс: <b>{new_balance}</b> слитков."
            )
            logger.info(f"Notified user {user_id} about {coins} coins addition")
        except Exception as e:
            logger.error(f"Failed to notify user {user_id} about coins addition: {e}")
            await message.answer(
                f"{INDENT}Не удалось уведомить пользователя <b>{user_id}</b>. Возможно, он заблокировал бота."
            )

        await message.answer(
            f"{INDENT}Начислено <b>{coins}</b> слитков пользователю с ID <b>{user_id}</b>."
        )
    except ValueError:
        await message.answer(
            f"{INDENT}Ошибка: введите данные в формате user_id:количество (например, 12345:10)."
        )
    except Exception as e:
        logger.error(f"Error adding coins: {e}")
        await message.answer(f"{INDENT}Произошла ошибка при начислении слитков: {str(e)}")

    await state.clear()

# Обработчик рассылки
@dp.callback_query(F.data == "broadcast")
async def start_broadcast(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return

    await state.set_state(AdminStates.waiting_for_broadcast)
    await callback.message.answer(
        f"{INDENT}Введите текст для рассылки всем пользователям.\n"
        f"{INDENT}Отправьте /cancel для отмены."
    )

@dp.message(AdminStates.waiting_for_broadcast)
async def process_broadcast(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer(f"{INDENT}Рассылка отменена.")
        return

    broadcast_text = message.text.strip()
    if not broadcast_text:
        await message.answer(f"{INDENT}Текст рассылки не может быть пустым.")
        return

    cursor.execute("SELECT user_id FROM users")
    users = cursor.fetchall()

    success = 0
    failed = 0

    for user in users:
        user_id = user[0]
        try:
            await bot.send_message(user_id, f"{INDENT}📢 Объявление:\n\n{INDENT}{broadcast_text}")
            success += 1
        except Exception as e:
            logger.error(f"Failed to send broadcast to user {user_id}: {e}")
            failed += 1

    await message.answer(
        f"{INDENT}Рассылка завершена.\n"
        f"{INDENT}Успешно отправлено: <b>{success}</b> пользователям.\n"
        f"{INDENT}Не удалось отправить: <b>{failed}</b> пользователям."
    )
    await state.clear()

# Обработчик управления категориями
@dp.callback_query(F.data == "categories")
async def manage_categories(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return

    cursor.execute("SELECT name, display_name FROM categories")
    categories = cursor.fetchall()

    text = f"{INDENT}📝 Управление категориями:\n\n"
    for idx, category in enumerate(categories, 1):
        name, display_name = category
        text += f"{INDENT}{idx}. {display_name} (системное имя: {name})\n"

    builder = InlineKeyboardBuilder()
    builder.add(
        InlineKeyboardButton(text="➕ Добавить категорию", callback_data="add_category"),
        InlineKeyboardButton(text="➖ Удалить категорию", callback_data="delete_category")
    )
    builder.adjust(2)

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "add_category")
async def add_category(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return

    await state.set_state(AdminStates.waiting_for_category)
    await callback.message.answer(
        f"{INDENT}Введите название новой категории в формате:\n"
        f"{INDENT}системное_имя:отображаемое_имя\n"
        f"{INDENT}Например: Новые_игры:Новые игры\n"
        f"{INDENT}Отправьте /cancel для отмены."
    )

@dp.message(AdminStates.waiting_for_category)
async def process_add_category(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.clear()
        await message.answer(f"{INDENT}Добавление категории отменено.")
        return

    try:
        name, display_name = message.text.strip().split(':')
        name = name.strip()
        display_name = display_name.strip()

        cursor.execute("SELECT name FROM categories WHERE name = ?", (name,))
        if cursor.fetchone():
            await message.answer(f"{INDENT}Категория с системным именем <b>{name}</b> уже существует.")
            return

        cursor.execute("INSERT INTO categories (name, display_name) VALUES (?, ?)", (name, display_name))
        conn.commit()
        await message.answer(
            f"{INDENT}Категория <b>{display_name}</b> (системное имя: {name}) успешно добавлена."
        )
    except ValueError:
        await message.answer(
            f"{INDENT}Ошибка: введите данные в формате системное_имя:отображаемое_имя."
        )
    except Exception as e:
        logger.error(f"Error adding category: {e}")
        await message.answer(f"{INDENT}Произошла ошибка при добавлении категории: {str(e)}")

    await state.clear()

@dp.callback_query(F.data == "delete_category")
async def delete_category(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("У вас нет прав администратора.", show_alert=True)
        return
    cursor.execute("SELECT name, display_name FROM categories")
    categories = cursor.fetchall()

    if not categories:
        await callback.message.answer(f"{INDENT}Нет доступных категорий для удаления.")
        return

    builder = InlineKeyboardBuilder()
    for category in categories:
        builder.add(InlineKeyboardButton(
            text=category[1],
            callback_data=f"confirm_delete_category_{category[0]}"
        ))
    builder.add(InlineKeyboardButton(
        text="⬅️ Назад",
        callback_data="categories"
    ))
    builder.adjust(2)

    await callback.message.edit_text(
        f"{INDENT}Выберите категорию для удаления:",
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data.startswith("confirm_delete_category_"))
async def confirm_delete_category(callback: types.CallbackQuery, state: FSMContext):
        if callback.from_user.id not in ADMIN_IDS:
            await callback.answer("У вас нет прав администратора.", show_alert=True)
            return

        category = callback.data.split("_")[3]

        # Проверяем, есть ли товары в категории
        cursor.execute(
            "SELECT COUNT(*) FROM accounts WHERE category = ? AND claimed_by IS NULL",
            (category,)
        )
        product_count = cursor.fetchone()[0]

        if product_count > 0:
            await callback.message.edit_text(
                f"{INDENT}Нельзя удалить категорию <b>{category}</b>, так как в ней есть товары ({product_count}).\n"
                f"{INDENT}Сначала удалите все товары из этой категории."
            )
            return

        try:
            cursor.execute("DELETE FROM categories WHERE name = ?", (category,))
            conn.commit()
            logger.info(f"Category {category} deleted successfully")
            await callback.message.edit_text(
                f"{INDENT}Категория <b>{category}</b> успешно удалена."
            )
        except Exception as e:
            logger.error(f"Error deleting category {category}: {e}")
            await callback.message.edit_text(
                f"{INDENT}Ошибка при удалении категории: {str(e)}"
            )

async def on_startup():
    logger.info("Bot is starting...")
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook deleted successfully")
    except Exception as e:
        logger.error(f"Error deleting webhook: {e}")

async def on_shutdown():
    logger.info("Bot is shutting down...")
    try:
        conn.close()
        logger.info("Database connection closed")
    except Exception as e:
        logger.error(f"Error closing database connection: {e}")
    await bot.session.close()
    logger.info("Bot session closed")

if __name__ == "__main__":
    import asyncio
    try:
        dp.startup.register(on_startup)
        dp.shutdown.register(on_shutdown)
        asyncio.run(dp.start_polling(bot))
    except Exception as e:
        logger.error(f"Error running bot: {e}")
        raise
