import asyncio

from aiogoogle import Aiogoogle  # noqa: F401, E402  imported but unused & module level import not at top of file
from aiogoogle.auth.creds import (  # noqa: E402 module level import not at top of file
    UserCreds,
    ClientCreds,
    ApiKey,
)  # noqa: F401  imported but unused

import json
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

from collections import deque

async def list_files(paths):
    async with Aiogoogle(user_creds=user_creds,
                         client_creds=client_creds) as aiogoogle:
        drive_v3 = await aiogoogle.discover("drive", "v3")

        parents = ["root"]

        while True:

            if len(paths) == 0:
                mode = "ls"
                q_str_tmpl = ("'{parent_id}' in parents "
                              "and trashed=false")
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


async def download_files(file_list, dirpath):
    async with Aiogoogle(user_creds=user_creds,
                         client_creds=client_creds) as aiogoogle:
        drive_v3 = await aiogoogle.discover('drive', 'v3')
        ff = [(f["name"], f["id"]) for f in file_list]
        for file_name, file_id in sorted(ff)[:3]:
            # file_id, file_name = f["id"], f["name"]
            path = os.path.join(dirpath, file_name)
            print(file_id, path)
            await aiogoogle.as_user(
                drive_v3.files.get(fileId=file_id,
                                   download_file=path, alt='media'),
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


# paths = deque(["igrins_data", "2020T3", "20201130"])
# selected_files = asyncio.run(list_files(paths))

asyncio.run(download_files(selected_files, "test"))
