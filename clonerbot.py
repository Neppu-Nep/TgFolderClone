import subprocess
import threading
import re
from time import sleep, monotonic

from telegram import Update, ParseMode
from telegram.constants import MAX_MESSAGE_LENGTH
from telegram.error import TimedOut, BadRequest, RetryAfter
from telegram.ext import Updater, CommandHandler, CallbackContext, run_async

import config

TOKEN = config.BOT_TOKEN
updater = Updater(TOKEN, use_context=True)
dp = updater.dispatcher
threads = threading.BoundedSemaphore(1)
allowed_chats = config.ALLOWED_CHATS
queue = []


@run_async
def clone(update: Update, context: CallbackContext):
    chat_id = str(update.effective_chat.id)
    user_id = str(update.message.from_user.id)
    bot = context.bot

    if chat_id not in allowed_chats:
        return

    args = context.args
    source = args[0]
    dest = args[1]
    try:
        thread_amount = args[2]
    except IndexError:
        thread_amount = 10
    try:
        view = args[3]
    except IndexError:
        view = 0

    start_time = monotonic()
    counter = 0
    bot.sendMessage(update.effective_chat.id, f"Added {source} -> {dest} to queue")
    queue.append({
        "source" : source,
        "dest" : dest,
        "user" : user_id,
        "threads" : thread_amount
    })

    threads.acquire()
    sleep(3)
    bot.sendMessage(update.effective_chat.id, "Started")
    cmd = f"py folderclone.py -s {source} -d {dest} -t {thread_amount} --view {view}"
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, universal_newlines=True)
    message = None
    to_edit_bool = True
    to_send = ""

    for line in proc.stdout:
        counter = int(monotonic() - start_time)

        if len(to_send) >= MAX_MESSAGE_LENGTH:
            to_edit_bool = False
            to_send = ""

        original = re.escape(line.rsplit("-", 1)[0])
        try:
            percent_update = line.rsplit("-", 1)[1].strip()
        except IndexError:
            percent_update = ""

        regex = f"({original})-.*"
        subst = f"\\1- {percent_update}"

        new_to_send, success = re.subn(regex, subst, to_send, flags=re.MULTILINE)

        if success:
            to_send = new_to_send
        else:
            to_send += f"{line}"
        to_send = to_send.replace("\n\n", "\n")

        if "BoundedSemaphore" in to_send:
            try:
                to_send = to_send.split("threads\n")[1]
            except IndexError:
                to_send = ""

        print(repr(line))

        if counter == 0:
            continue

        try:
            if message:
                if not to_edit_bool:
                    to_edit_bool = True
                    message = bot.sendMessage(update.effective_chat.id, to_send)
                else:
                    message = message.edit_text(to_send)
            else:
                message = bot.sendMessage(update.effective_chat.id, to_send)
        except (TimedOut, RetryAfter, BadRequest):
            print(to_send)
        counter = 0
        start_time = monotonic()
        proc.stdout.flush()

    bot.sendMessage(update.effective_chat.id, "Done")
    sleep(10)
    queue.pop(0)
    threads.release()


@run_async
def status(update: Update, context: CallbackContext):
    chat_id = str(update.effective_chat.id)

    if chat_id not in allowed_chats:
        return

    message = ""

    if not queue:
        update.message.reply_text("No jobs currently running.")
        return

    for each_entry_num, each_entry in enumerate(queue):
        source = each_entry['source']
        dest = each_entry['dest']
        user = each_entry['user']
        thread_used = each_entry['threads']

        if each_entry_num == 0:
            message += f"Current Job - From `{source}` to `{dest}` started by `{user}`\nTotal thread used : `{thread_used}`\n\n"
        else:
            message += f"Queue {each_entry_num+1} - From `{source}` to `{dest}` started by `{user}`\nTotal thread to use : `{thread_used}`\n\n"

    update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)


dp.add_handler(CommandHandler('clone', clone))
dp.add_handler(CommandHandler('status', status))
print("Bot Started.")
updater.start_polling()
updater.idle()
