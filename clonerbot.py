import subprocess
import threading
import re
import os
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
    bot.sendMessage(update.effective_chat.id, "Started", timeout=5)
    if 'DYNO' in os.environ:
        cmd = f"python3 folderclone.py -s {source} -d {dest} --threads {thread_amount}"
    else:
        cmd = f"py folderclone.py -s {source} -d {dest} --threads {thread_amount}"
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, universal_newlines=True)
    message = None
    to_edit_bool = True
    to_send = ""

    for line in proc.stdout:
        counter = int(monotonic() - start_time)

        if len(to_send) >= MAX_MESSAGE_LENGTH:
            to_edit_bool = False
            to_send = ""

        matches = re.match(r"(.*)( -.*| \|.*)|(.*)", line.strip(), flags=re.MULTILINE)

        original = matches.group(1)
        if not original:
            original = matches.group(3)
        original = re.escape(original)

        percent_update = matches.group(2)
        if percent_update:
            percent_update = percent_update.rstrip()

        regex = f"({original})( -.*| \\|.*)|({original})"
        subst = f"\\1\\3{percent_update}"

        new_to_send, success = re.subn(regex, subst, to_send, flags=re.MULTILINE)

        if success:
            to_send = new_to_send
        else:
            to_send += f"{line}"
        to_send = to_send.replace("\n\n", "\n")

        print(repr(line))

        if re.match(r"Copying from (.*) to (.*)", line):
            if message:
                if not to_edit_bool:
                    bot.sendMessage(update.effective_chat.id, to_send)
                else:
                    message.edit_text(to_send)
            else:
                bot.sendMessage(update.effective_chat.id, to_send)
            counter = 0
            to_edit_bool = False
            to_send = ""
            continue

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
    try:
        message.edit_text(to_send)
    except BadRequest:
        pass


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
            message += f"Queue {each_entry_num} - From `{source}` to `{dest}` started by `{user}`\nTotal thread to use : `{thread_used}`\n\n"

    update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)


dp.add_handler(CommandHandler('clone', clone))
dp.add_handler(CommandHandler('status', status))
print("Bot Started.")
updater.start_polling()
updater.idle()
