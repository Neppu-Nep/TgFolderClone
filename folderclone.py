import time
import threading
import json
import socket
import random
import os

from argparse import ArgumentParser
from glob import glob
from httplib2shim import patch
from urllib3.exceptions import ProtocolError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from google.auth.exceptions import TransportError

from CounterProgress import CounterProgress

class MultiFolderClone():
    patch()
    source = ''
    dest = []
    accounts = []
    path = 'accounts'
    width = 2
    thread_count = None
    skip_bad_dests = False

    drive_to_use = 1
    files_to_copy = []
    encoding = None
    threads = None
    id_whitelist = None
    id_blacklist = None
    name_whitelist = None
    name_blacklist = None
    share_publicly = False
    file_copy_error = 0
    bad_drives = []
    google_opts = ['trashed = false']
    override_thread_check = False
    verbose = False
    max_retries = 3
    sleep_time = 1
    dont_recurse = False

    statistics = {
        'folders': 0,
        'files': 0,
        'total_accounts': 0,
        'quotad_accounts': 0,
        'errors': {},
    }

    error_codes = {
        'dailyLimitExceeded': True,
        'userRateLimitExceeded': True,
        'rateLimitExceeded': True,
        'sharingRateLimitExceeded': True,
        'appNotAuthorizedToFile': True,
        'insufficientFilePermissions': True,
        'domainPolicy': True,
        'backendError': True,
        'internalError': True,
        'badRequest': False,
        'invalidSharingRequest': False,
        'authError': False,
        'notFound': False,
        'failedPrecondition': True
    }

    def __init__(self, source, dest, **options):
        self.source = source
        self.dest = dest
        if isinstance(dest, str):
            self.dest = [dest]
        if options.get('encoding') is not None:
            self.encoding = options['encoding']
        if options.get('thread_count') is not None:
            self.thread_count = int(options['thread_count'])
        if options.get('skip_bad_dests') is not None:
            self.skip_bad_dests = bool(options['skip_bad_dests'])
        if options.get('path') is not None:
            self.path = str(options['path'])
        if options.get('width') is not None:
            self.width = int(options['width'])
        if options.get('sleep_time') is not None:
            self.sleep_time = int(options['sleep_time'])
        if options.get('max_retries') is not None:
            self.max_retries = int(options['max_retries'])
        if options.get('id_whitelist') is not None:
            self.id_whitelist = list(options['id_whitelist'])
        if options.get('name_whitelist') is not None:
            self.name_whitelist = list(options['name_whitelist'])
        if options.get('id_blacklist') is not None:
            self.id_blacklist = list(options['id_blacklist'])
        if options.get('name_blacklist') is not None:
            self.name_blacklist = list(options['name_blacklist'])
        if options.get('override_thread_check') is not None:
            self.override_thread_check = bool(options['override_thread_check'])
        if options.get('verbose') is not None:
            self.verbose = bool(options['verbose'])
        if options.get('google_opts') is not None:
            self.google_opts = list(options['google_opts'])
        if options.get('no_recursion') is not None:
            self.dont_recurse = bool(options['no_recursion'])
        if options.get('share_publicly') is not None:
            self.share_publicly = bool(options['share_publicly'])

        self.accounts = glob(self.path + '/*.json')
        if not self.accounts:
            raise ValueError('The path provided (%s) has no accounts.' % self.path)

    def _add_error_stats(self, reason):
        if reason in self.statistics['errors']:
            self.statistics['errors'][reason] += 1
        else:
            self.statistics['errors'][reason] = 1

    def _create_drive(self):
        while True:
            random_acc = random.choice(self.accounts)
            try:
                credentials = Credentials.from_service_account_file(random_acc, scopes=[
                    "https://www.googleapis.com/auth/drive"
                ])
                random_drive = build("drive", "v3", credentials=credentials)
            except HttpError:
                print("#Error SA Error")
            else:
                break
        return (random_acc, random_drive)

    def _log(self, line):
        if self.verbose:
            print(line)

    def _apicall(self, request):
        resp = None
        tries = 0

        while True:
            tries += 1
            if tries > self.max_retries:
                return None
            try:
                resp = request.execute()
            except HttpError as error:
                try:
                    error_details = json.loads(error.content.decode('utf-8'))
                except json.decoder.JSONDecodeError:
                    time.sleep(self.sleep_time)
                    continue
                reason = error_details['error']['errors'][0]['reason']
                # self._add_error_stats(reason)
                if reason == 'userRateLimitExceeded':
                    return False
                elif reason == 'storageQuotaExceeded':
                    print('Got storageQuotaExceeded error. You are not using a Shared Drive.')
                    return False
                elif reason == "cannotCopyFile":
                    self.file_copy_error += 1
                    return True
                elif reason == 'teamDriveFileLimitExceeded':
                    raise RuntimeError('The Shared Drive is full. No more files can be copied to it.')
                elif self.error_codes[reason]:
                    time.sleep(self.sleep_time)
                    continue
                else:
                    return None
            except (socket.error, ProtocolError, TransportError) as err:
                reason = str(err)
                # self._add_error_stats(reason)
                time.sleep(self.sleep_time)
                continue
            else:
                return resp

    def _ls(self, service, parent, searchTerms=[]):
        files = []
        resp = {'nextPageToken' : None}
        while 'nextPageToken' in resp:
            resp = self._apicall(
                service.files().list(
                    q=' and '.join(['"%s" in parents' % parent] + self.google_opts + searchTerms),
                    fields='files(md5Checksum,id,name),nextPageToken',
                    pageSize=1000,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    pageToken=resp['nextPageToken']
                )
            )
            files += resp['files']
        return files

    def _lsd(self, service, parent):
        return self._ls(
            service,
            parent,
            searchTerms=['mimeType contains "application/vnd.google-apps.folder"']
        )

    def _lsf(self, service, parent):
        return self._ls(
            service,
            parent,
            searchTerms=['not mimeType contains "application/vnd.google-apps.folder"']
        )

    def _copy(self, sa_name, driv, source, dest):
        self._log('Copying file %s into folder %s' % (source, dest))
        resp = self._apicall(driv.files().copy(fileId=source, body={'parents': [dest]}, supportsAllDrives=True))
        if not resp:
            self._log('Error: Quotad SA')
            self.bad_drives.append(sa_name)
            self.files_to_copy.append((source, dest))
        elif self.share_publicly:
            self._apicall(driv.permissions().create(fileId=resp['id'], body={'role':'reader', 'type':'anyone'}, supportsAllDrives=True))
        self.threads.release()

    def _rcopy(self, source, dest, folder_name, display_line, width):
        list_drive = self._create_drive()[1]
        self._log('%s to %s' % (source, dest))
        files_source = self._lsf(list_drive, source)
        files_dest = self._lsf(list_drive, dest)
        folders_source = self._lsd(list_drive, source)
        folders_dest = self._lsd(list_drive, dest)
        files_to_copy = []
        files_source_id = []
        files_dest_id = []
        folder_len = len(folders_source) - 1

        self._log('Found %d files in source.' % len(files_source))
        self._log('Found %d folders in source.' % len(folders_source))
        self._log('Found %d files in dest.' % len(files_dest))
        self._log('Found %d folders in dest.' % len(folders_dest))

        folders_copied = {}
        for file in files_source:
            files_source_id.append(dict(file))
            file.pop('id')
        for file in files_dest:
            files_dest_id.append(dict(file))
            file.pop('id')

        i = 0
        while len(files_source) > i:
            if files_source[i] not in files_dest:
                files_to_copy.append(files_source_id[i])
            i += 1

        self._log('Checking whitelist and blacklist')
        for i in list(files_to_copy):
            if self.id_whitelist is not None:
                if i['id'] not in self.id_whitelist:
                    files_to_copy.remove(i)
            if self.id_blacklist is not None:
                if i['id'] in self.id_blacklist:
                    files_to_copy.remove(i)
            if self.name_whitelist is not None:
                if i['name'] not in self.name_whitelist:
                    files_to_copy.remove(i)
            if self.name_blacklist is not None:
                if i['name'] in self.name_blacklist:
                    files_to_copy.remove(i)

        self._log('Added %d files to copy list.' % len(files_to_copy))

        self.files_to_copy = [(i['id'], dest) for i in files_to_copy]

        self._log('Copying files')

        fullname = display_line + folder_name
        if 'DYNO' not in os.environ:
            fullname = fullname.encode(self.encoding, errors='replace').decode(self.encoding)

        if files_to_copy:
            while self.files_to_copy:
                files_to_copy = self.files_to_copy
                self.files_to_copy = []
                running_threads = []

                pbar = CounterProgress(f"{fullname}", max=len(files_to_copy), encoding=self.encoding)
                pbar.update()

                # copy
                for i in files_to_copy:
                    self.threads.acquire()
                    random_acc, random_drive = self._create_drive()
                    thread = threading.Thread(
                        target=self._copy,
                        args=(random_acc, random_drive, i[0], i[1])
                    )
                    running_threads.append(thread)
                    thread.start()
                    pbar.next()

                if self.file_copy_error:
                    pbar.finish_update_with_error(self.file_copy_error)
                    self.file_copy_error = 0
                else:
                    pbar.finish_update()
                pbar.finish()

                # join all threads
                for i in running_threads:
                    i.join()

                # check for bad drives
                for i in self.bad_drives:
                    if i in self.accounts:
                        self.accounts.remove(i)
                self.bad_drives = []

                # If there is less than 2 SAs, exit
                if len(self.accounts) == 1:
                    raise RuntimeError('Out of SAs.')

            # copy completed
            #print(display_line + folder_name + ' | Synced')
        elif files_source and len(files_source) <= len(files_dest):
            print(fullname + ' | Up to date')
        else:
            print(fullname)

        for i in folders_dest:
            folders_copied[i['name']] = i['id']

        current_folder = 0
        if self.dont_recurse:
            return

        for folder in folders_source:
            if current_folder == folder_len:
                next_display_line = display_line.replace('├' + '─' * width + ' ', '│' + ' ' * width + ' ').replace('└' + '─' * width + ' ', '  ' + ' ' * width) + '└' + '─' * width + ' '
            else:
                next_display_line = display_line.replace('├' + '─' * width + ' ', '│' + ' ' * width + ' ').replace('└' + '─' * width + ' ', '  ' + ' ' * width) + '├' + '─' * width + ' '
            if folder['name'] not in folders_copied.keys():
                folder_id = self._apicall(
                    list_drive.files().create(
                        body={
                            'name': folder['name'],
                            'mimeType': 'application/vnd.google-apps.folder',
                            'parents': [dest]
                        },
                        supportsAllDrives=True
                    )
                )['id']
            else:
                folder_id = folders_copied[folder['name']]
            self._rcopy(
                folder['id'],
                folder_id,
                folder['name'].replace('%', '%%'),
                next_display_line,
                width
            )
            current_folder += 1
        return

    def clone(self):

        check = self._create_drive()[1]

        try:
            root_dir = check.files().get(fileId=self.source, supportsAllDrives=True).execute()['name']
        except HttpError:
            raise ValueError('Source folder %s cannot be read or is invalid.' % self.source)

        dest_dict = {i:'' for i in self.dest}
        for key in list(dest_dict.keys()):
            try:
                dest_dir = check.files().get(fileId=key, supportsAllDrives=True).execute()['name']
                dest_dict[key] = dest_dir
            except HttpError:
                if not self.skip_bad_dests:
                    raise ValueError('Destination folder %s cannot be read or is invalid.' % key)
                else:
                    dest_dict.pop(key)

        print('Using %d Drive Services' % len(self.accounts))
        if self.thread_count is not None and self.thread_count <= len(self.accounts):
            self.threads = threading.BoundedSemaphore(self.thread_count)
            print('BoundedSemaphore with %d threads' % self.thread_count)
        elif self.thread_count is None:
            self.threads = threading.BoundedSemaphore(len(self.accounts))
            print('BoundedSemaphore with %d threads' % len(self.accounts))
        else:
            raise ValueError('More threads than there is service accounts.')

        for i, dest_dir in dest_dict.items():
            print('Copying from %s to %s.' % (root_dir, dest_dir))
            self._rcopy(self.source, i, root_dir, '', self.width)

def main():
    parse = ArgumentParser(description='A tool intended to copy large files from one folder to another.')
    parse.add_argument('--encoding', '-e', type=str, default=None, help='Set the encoding.')
    parse.add_argument('--width', '-w', type=int, default=1, help='Set the width of the view option.')
    parse.add_argument('--path', '-p', default='accounts', help='Specify an alternative path to the service accounts.')
    parse.add_argument('--threads', type=int, default=None, help='Specify a different thread count. Cannot be greater than the amount of service accounts available.')
    parse.add_argument('--skip-bad-dests', default=False, action='store_true', help='Skip any destionations that cannot be read.')
    parse.add_argument('--no-recursion', default=False, action='store_true', help='Do not recursively copy folders.')
    parse.add_argument('--verbose', default=False, action='store_true', help='Verbose output. WARNING: Very verbose.')
    parse.add_argument('--force-threads', default=True, action='store_true', help='Overrides the thread limit check.')
    parse.add_argument('--share-publicly', default=False, action='store_true', help='Shares the files publicly after coyping.')
    parsereq = parse.add_argument_group('required arguments')
    parsereq.add_argument('--source-id', '--source', '-s', help='The source ID of the folder to copy.', required=True)
    parsereq.add_argument('--destination-id', '--destination', '-d', action='append', help='The destination ID of the folder to copy to.', required=True)
    args = parse.parse_args()
    mfc = MultiFolderClone(
        source=args.source_id,
        dest=args.destination_id,
        path=args.path,
        width=args.width,
        thread_count=args.threads,
        no_recursion=args.no_recursion,
        verbose=args.verbose,
        skip_bad_dests=args.skip_bad_dests,
        override_thread_check=args.force_threads,
        share_publicly=args.share_publicly,
        encoding=args.encoding
    )
    try:
        mfc.clone()
    except ValueError as err:
        print(err)
        if str(err) == 'More threads than there is service accounts.':
            print('Use --force-threads to override this check.')
        else:
            raise err
    except KeyboardInterrupt:
        print('Quitting.')

main()
