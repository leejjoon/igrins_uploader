import os
from collections import deque

import asyncio

from aiogoogle import Aiogoogle
from aiogoogle.excs import HTTPError
from aiogoogle.auth.creds import (
    UserCreds,
    ClientCreds,
    ApiKey,
)

import json
import datetime
from pathlib import Path
import glob
import hashlib

credfile = "igrins_upload_cred.txt"
config = json.load(open(credfile))

user_creds = UserCreds(
    access_token=config["access_token"],
    refresh_token=config["refresh_token"],
    expires_at=None if "expires_at" not in config else config["expires_at"]
)

# user_creds = {'access_token': 'an_access_token'}

client_creds = ClientCreds(
    client_id=config["client_id"],
    client_secret=config["client_secret"],
    scopes=config["scopes"],
)

def get_timemark():
    timemark = datetime.datetime.isoformat(datetime.datetime.utcnow()) + "Z"
    return timemark


async def list_files(paths, parents=None):
    async with Aiogoogle(user_creds=user_creds,
                         client_creds=client_creds) as aiogoogle:
        drive_v3 = await aiogoogle.discover("drive", "v3")

        if parents is None:
            parents = ["root"]

        while True:

            if len(paths) == 0:
                mode = "ls"
                q_str_tmpl = ("'{parent_id}' in parents "
                              "and trashed=false")
                dirname = None
            else:
                mode = "cd"
                q_str_tmpl = ("'{parent_id}' in parents "
                              "and mimeType contains 'application/vnd.google-apps.folder' "
                              "and trashed=false")
                dirname = paths.popleft()

            print(mode, dirname)
            selected_files = []
            for parent_id in parents:
                q_str = q_str_tmpl.format(parent_id=parent_id)


                # _ = {"q": q_str}
                res = await aiogoogle.as_user(drive_v3.files.list(q=q_str),
                                              full_res=True)

                async for page in res:
                    for file in page["files"]:
                        if mode == "cd" and file.get("name") == dirname:
                            selected_files.append(file.get("id"))
                            # print(f"{file.get('id')}: {file.get('name')}")
                        elif mode == "ls":
                            selected_files.append(file)

            if mode == "cd":
                if len(parents) == 0:
                    raise RuntimeError(f"no direcotry found: {dirname}")
                parents = deque(selected_files)
            else:
                break

        # print(selected_files)
        # print([file["name"] for file in selected_files])

        return selected_files


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield i, lst[i:i + n]


async def fetch_files_metadata(file_list, dirpath):
    async with Aiogoogle(user_creds=user_creds,
                         client_creds=client_creds) as aiogoogle:
        drive_v3 = await aiogoogle.discover('drive', 'v3')
        ff = [(f["name"], f) for f in file_list]

        metadata_list = []
        fields = "id,name,mimeType,md5Checksum"
        metadata_dir = os.path.join(dirpath, "_METADATA_")
        print(metadata_dir)

        metadata_dict = dict()
        list_to_fetch = []
        for n, m in sorted(ff):
            fn_metadata_candi = "{name}_{timemark}.json".format(name=m["name"],
                                                                timemark="*")
            fnlist = glob.glob(os.path.join(metadata_dir,
                                            fn_metadata_candi))
            if len(fnlist):
                fn_metadata = max(fnlist)
                metadata_dict[n] = json.load(open(fn_metadata))
            else:
                list_to_fetch.append((n, m))

        timemark = get_timemark()

        Path(metadata_dir).mkdir(parents=True, exist_ok=True)

        for i, chunk in chunks(list_to_fetch, 10):
            batches = [drive_v3.files.get(fileId=m["id"],
                                          fields=fields) for _, m
                       in chunk]
            # print(i, len(batches))
            do_repeat = True
            while do_repeat:
                try:
                    metadata_chunk = await aiogoogle.as_user(*batches)
                except HTTPError:
                    print("E", end="")
                    await asyncio.sleep(1.)
                else:
                    do_repeat = False

            for m in metadata_chunk:
                fn_metadata = "{name}_{timemark}.json".format(name=m["name"],
                                                              timemark=timemark)
                json.dump(m, open(os.path.join(metadata_dir,
                                               fn_metadata), "w"))

                # fn = os.path.join(dirpath, m['namee'])
                # await aiogoogle.as_user(
                #     drive_v3.files.get(fileId=m["id"],
                #                        download_file=fn,
                #                        alt='media'),
                # )

            metadata_dict.update((m["name"], m) for m in metadata_chunk)
            print(".", end="")

        print("")

    return metadata_dict


async def download_files(file_list, dirpath):
    async with Aiogoogle(user_creds=user_creds,
                         client_creds=client_creds) as aiogoogle:
        drive_v3 = await aiogoogle.discover('drive', 'v3')
        ff = [(f["name"], f) for f in file_list]
        for file_name, f in sorted(ff):
            # file_id, file_name = f["id"], f["name"]
            fn = os.path.join(dirpath, file_name)

            if os.path.exists(fn):
                md5sum = hashlib.md5(open(fn, "rb").read()).hexdigest()
                if md5sum == f["md5Checksum"]:
                    print(fn, "skip")
                    continue

            print(fn, "downloading")
            await aiogoogle.as_user(
                drive_v3.files.get(fileId=f["id"],
                                   download_file=fn, alt='media'),
            )

# asyncio.run(download_file('abc123', '/home/user/Desktop/my_file.zip'))


# if False
#     async with Aiogoogle(user_creds=user_creds) as aiogoogle:
#         drive_v3 = await aiogoogle.discover('drive', 'v3')
#         json_res = await aiogoogle.as_user(
#             drive_v3.files.list(),
#         )
#         for file in json_res['files']:
#             print(file['name'])


if __name__ == '__main__':
    main()

from enum import Enum



class GDriveHelper(object):
    OBSDATE_STATUS = Enum("OBSDATE_STATUS", "NOT_EXISTS VERIFIED")
    FILE_STATUS = Enum("FILE_STATUS", "NOT_EXISTS VERIFIED")

    def __init__(self, root_dir="."):
        self.root_dir = root_dir
        self.metadata_dir = "_METADATA_"

    def get_obsdate_dir(self, trimester, obsdate):
        dirname = os.path.join(self.root_dir, trimester, obsdate)
        return dirname

    def iter_obsdate_status(self, trimester, metadata):
        for obsdate, m in sorted((m["name"], m) for m in metadata):
            dirname = self.get_obsdate_dir(trimester, obsdate)
            status_file = os.path.join(dirname,
                                       self.metadata_dir,
                                       "STATUS")
            if os.path.exists(status_file):
                status = self.OBSDATE_STATUS[open(status_file).read().strip()]
            else:
                status = self.OBSDATE_STATUS["NOT_EXISTS"]

            yield obsdate, m, status

    def iter_file_status(self, trimester, obsdate, metadata):
        for fn, m in sorted((m["name"], m) for m in metadata):
            # dirname = self.get_obsdate_dir(trimester, obsdate)
            # status_file = os.path.join(dirname,
            #                            self.metadata_dir,
            #                            "STATUS")
            # if os.path.exists(status_file):
            #     status = self.OBSDATE_STATUS[open(status_file).read().strip()]
            # else:
            #     status = self.OBSDATE_STATUS["NOT_EXISTS"]
            status = self.FILE_STATUS["NOT_EXISTS"]

            yield fn, m, status

    async def check_trimester(self, trimester, show_status=True):
        # trimester = "2020T3"
        paths = deque(["igrins_data", trimester])
        selected_files = await list_files(paths)

        kw = dict(trimester=trimester, timemark=get_timemark())
        fn = os.path.join(self.root_dir, self.metadata_dir,
                          "{trimester}_{timemark}.json").format(**kw)

        Path(os.path.dirname(fn)).mkdir(parents=True, exist_ok=True)

        json.dump(selected_files, open(fn, "w"))

        if show_status:
            for obsdate, m, s in self.iter_obsdate_status(trimester,
                                                          selected_files):

                print("{} - {}".format(obsdate, s))

        return selected_files

    def load_most_recent_trimester_metadata(self, trimester):
        kw = dict(trimester=trimester, timemark="*")
        fn = os.path.join(self.root_dir, self.metadata_dir,
                          "{trimester}_{timemark}.json").format(**kw)
        import glob
        fnlist = glob.glob(fn)
        if len(fnlist):
            fn = max(fnlist)
            return json.load(open(fn))

    async def sync_trimester(self, trimester, recheck_gdrive=False):
        if recheck_gdrive:
            metadata = await check_trimester(self, trimester,
                                             show_status=False)
        else:
            metadata = self.load_most_recent_trimester_metadata(trimester)

        for obsdate, m, s in self.iter_obsdate_status(trimester,
                                                      metadata):

            print("{} - {}".format(obsdate, s))
            metadata = await self.check_obsdate(trimester, obsdate, m)

    async def check_obsdate(self, trimester, obsdate, m,
                            show_status=True):
        # m : data for obsdate from trimester metadata

        kw = dict(trimester=trimester, obsdate=obsdate,
                  timemark=get_timemark())

        fn = os.path.join(self.root_dir, self.metadata_dir,
                          "{trimester}",
                          "{obsdate}_{timemark}.json").format(**kw)

        Path(os.path.dirname(fn)).mkdir(parents=True, exist_ok=True)

        filelist = await list_files([], [m["id"]])

        json.dump(filelist, open(fn, "w"))

        if show_status:
            for fn, m, s in self.iter_file_status(trimester, obsdate, filelist):
                print(fn, s)

    def load_most_recent_obsdate_metadata(self, trimester, obsdate):
        kw = dict(trimester=trimester, obsdate=obsdate,
                  timemark="*")

        fn = os.path.join(self.root_dir, self.metadata_dir,
                          "{trimester}",
                          "{obsdate}_{timemark}.json").format(**kw)

        import glob
        fnlist = glob.glob(fn)
        if len(fnlist):
            fn = max(fnlist)
            return json.load(open(fn))

    async def sync_obsdate(self, trimester, obsdate, recheck_gdrive=False):
        if recheck_gdrive:
            metadata = await check_obsdate(self, trimester, obsdate,
                                           show_status=False)
        else:
            metadata = self.load_most_recent_obsdate_metadata(trimester,
                                                              obsdate)
        dirpath = self.get_obsdate_dir(trimester, obsdate)
        metadata_dict = await fetch_files_metadata(metadata, dirpath)

        file_list = [metadata_dict[k] for k in sorted(metadata_dict.keys())]
        await download_files(file_list, dirpath)

        # for fn, m, s in self.iter_file_status(trimester, obsdate, metadata):
        #     print(fn, s)

    async def sync_files(self, trimester, obsdate, recheck_gdrive=False):
        if recheck_gdrive:
            metadata = await check_obsdate(self, trimester, obsdate,
                                           show_status=False)
        else:
            metadata = self.load_most_recent_obsdate_metadata(trimester,
                                                              obsdate)

        for fn, m, s in self.iter_file_status(trimester, obsdate, metadata):
            print(fn, s)

def main():
    gd = GDriveHelper(root_dir=".")
    trimester = "2020T3"
    asyncio.run(gd.check_trimester(trimester))

if True:
    gd = GDriveHelper(root_dir=".")
    trimester = "2020T3"
    obsdate = "20201102"
    # k = gd.load_most_recent_trimester_metadata(trimester)
    asyncio.run(gd.sync_obsdate(trimester, obsdate))


def main_old():
    paths = deque(["igrins_data", "2020T3"])
    selected_files = asyncio.run(list_files(paths))

    vv = {}
    for f in selected_files:
        vv.setdefault(f["name"], []).append(f)

    filelist_name_tmpl = "igrins_gdrive_{}.json"

    # In case there are multiple direcotry with same names, just pick last
    # ones.
    dirs_to_fetch = []
    for name, v in vv.items():
        filelist_name = filelist_name_tmpl.format(name)
        if os.path.exists(filelist_name):
            continue

        print(name, v[-1])
        dirs_to_fetch.append((name, v[-1]))

    for obsdate, fileinfo in dirs_to_fetch:
        #obsdate = dirs_to_fetch[0][0]
        #fileinfo = dirs_to_fetch[0][-1]
        print(obsdate)

        filelist = asyncio.run(list_files([], [fileinfo["id"]]))

        metadata_list = asyncio.run(fetch_files_metadata(filelist))

        filelist_name = filelist_name_tmpl.format(obsdate)
        json.dump(metadata_list, open(filelist_name, "w"))

    dirpath = obsdate
    asyncio.run(download_files(metadata_list, dirpath))
    # asyncio.run(download_files(selected_files, "test"))


    obsdate = "20201130"
    fnname = "SDCS_20201130_0303.fits"
    fn = "{}/{}".format(obsdate, fnname)
    md5sum = hashlib.md5(open(fn, "rb").read()).hexdigest()

    filelist_name = filelist_name_tmpl.format(obsdate)
    metadata_list = json.load(open(filelist_name, "r"))
    metadata_dict = dict((m["name"], m) for m in metadata_list)

    if metadata_dict[fnname]["md5Checksum"] != md5sum:
        print("incorrect md5sum: ", fnname)
