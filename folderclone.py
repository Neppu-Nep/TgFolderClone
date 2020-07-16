import time
import threading
import glob
import argparse
import json
import random
import socket

from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from CounterProgress import CounterProgress

stt = time.time()
views = ["tree", "basic"]
args = {}

parse = argparse.ArgumentParser(description='A tool intended to copy large files from one folder to another.')
parse.add_argument('--view', default=0, help='Set the view to a different setting (tree[0]|basic[1]).')
parse.add_argument('--thread', '-t', default=50, help='Specify total number of threads to use.')
parse.add_argument('--skip', default=None, help='Folder ID mark')
parsereq = parse.add_argument_group('required arguments')
parsereq.add_argument('--source-id', '-s', help='The source ID of the folder to copy.', required=True)
parsereq.add_argument('--destination-id', '-d', help='The destination ID of the folder to copy to.', required=True)
args = parse.parse_args()

view = int(args.view)
thread_num = int(args.thread)
skip = args.skip
print('Copy from %s to %s.' % (args.source_id, args.destination_id))
print('View set to %s (%s).' % (view, views[view]))

def ls(parent, searchTerms=""):
    while True:
        random_drive = generaterandomdrive()
        try:
            files = []
            resp = random_drive.files().list(q=f"'{parent}' in parents{searchTerms}", pageSize=1000, supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            files += resp["files"]
            while "nextPageToken" in resp:
                resp = random_drive.files().list(q=f"'{parent}' in parents" + searchTerms, pageSize=1000, supportsAllDrives=True, includeItemsFromAllDrives=True, pageToken=resp["nextPageToken"]).execute()
                files += resp["files"]
            return files
        except HttpError:
            print("#Error Listing")
            time.sleep(3)

def list_folder(parent):

    return ls(parent, searchTerms=" and mimeType contains 'application/vnd.google-apps.folder'")

def list_file(parent):

    return ls(parent, searchTerms=" and not mimeType contains 'application/vnd.google-apps.folder'")

def generaterandomdrive():

    global accsf

    socket.setdefaulttimeout(600)
    acc_thread.acquire()
    random_acc = random.choice(accsf)
    while True:
        try:
            credentials = Credentials.from_service_account_file(random_acc, scopes=[
                "https://www.googleapis.com/auth/drive"
            ])
            random_drive = build("drive", "v3", credentials=credentials)
        except HttpError:
            random_acc = random.choice(accsf)
            print("#Error SA Error")
        else:
            break

    acc_thread.release()
    return random_drive

def copy(source, dest):

    while True:
        random_drive = generaterandomdrive()
        try:
            random_drive.files().copy(fileId=source, body={"parents": [dest]}, supportsAllDrives=True).execute()
        except HttpError as err:
            reason = json.loads(err.content).get('error').get('errors')[0].get('reason')
            reason_list = ["dailyLimitExceeded", "userRateLimitExceeded", "sharingRateLimitExceeded"]
            if reason not in reason_list:
                print(f"#Error {reason}")
            time.sleep(3)
        else:
            break
    time.sleep(20)
    threads.release()

def rcopy(source, dest, sname, pre):

    global skip

    pres = pre
    if view == 1:
        pres = ""

    filestocopy = list_file(source)

    if skip:
        if source != skip:
            if filestocopy:
                print(f"{pre}{sname} - Skipped")
        else:
            skip = None

    if filestocopy:
        if not skip:
            fullname = pres + sname
            pbar = CounterProgress(f"{fullname[:40]} ({source}) - ", max=len(filestocopy))
            pbar.update()
            for i in filestocopy:
                threads.acquire()
                thread = threading.Thread(target=copy, args=(i["id"], dest))
                thread.start()
                pbar.next()

            pbar.finish()
    else:
        print(f"{pres}{sname} ({source})")

    folderstocopy = list_folder(source)
    folderlen = len(folderstocopy) - 1
    currentlen = 0
    for i in folderstocopy:
        pre = pre.replace(f"├─ ", f"│ ")
        nstu = pre.replace(f"└─ ", f" ")
        if currentlen == folderlen:
            nstu += f"└─ "
        else:
            nstu += f"├─ "
        if not skip:
            while True:
                random_drive = generaterandomdrive()
                try:
                    resp = random_drive.files().create(body={
                        "name": i["name"],
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": [dest]
                    }, supportsAllDrives=True).execute()
                except HttpError:
                    print("#Error Create Error")
                else:
                    break
        else:
            while True:
                random_drive = generaterandomdrive()
                try:
                    td_id = random_drive.files().get(
                        fileId=dest,
                        supportsAllDrives=True
                    ).execute()["driveId"]
                except HttpError:
                    print("#Error Get Error")
                else:
                    break
            while True:
                random_drive = generaterandomdrive()
                name = i['name'].replace("'", "\'")
                try:
                    resp = random_drive.files().list(
                        corpora="drive",
                        driveId=td_id,
                        includeItemsFromAllDrives=True,
                        q=f"name = '{name}'",
                        supportsAllDrives=True
                    ).execute()['files'][0]
                except HttpError:
                    print("#Error Find Error")
                except IndexError:
                    random_drive = generaterandomdrive()
                    try:
                        resp = random_drive.files().create(body={
                            "name": i["name"],
                            "mimeType": "application/vnd.google-apps.folder",
                            "parents": [dest]
                        }, supportsAllDrives=True).execute()
                    except HttpError:
                        print("#Error Create Error")
                    else:
                        break
                else:
                    break
        rcopy(i["id"], resp["id"], i["name"].replace('%', "%%"), nstu)
        currentlen += 1

accsf = glob.glob('accounts/*.json')
acc_thread = threading.BoundedSemaphore(1)
threads = threading.BoundedSemaphore(thread_num)
print('BoundedSemaphore with %d threads' % thread_num)

try:
    rcopy(args.source_id, args.destination_id, "root", "")
except KeyboardInterrupt:
    print('Quitting')
except Exception as e:
    print(e)

print('Complete.')
hours, rem = divmod((time.time() - stt), 3600)
minutes, sec = divmod(rem, 60)
print("Elapsed Time:\n{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), sec))
