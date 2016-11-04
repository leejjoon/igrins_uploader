from pydrive.auth import GoogleAuth

def authorize(credfile):

    gauth = GoogleAuth()

    gauth.LoadCredentialsFile(credfile)
    if gauth.credentials is None:
        # Authenticate if they're not there
        #gauth.LocalWebserverAuth()
        gauth.CommandLineAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()
        # Save the current credentials to a file
    gauth.SaveCredentialsFile(credfile)

    return gauth


def list_files(drive, parent):
    if parent is None:
        parent_id = "root"
    else:
        parent_id = parent['id']

    q_str = "'{}' in parents and trashed=false".format(parent_id)

    file_list = drive.ListFile({'q': q_str}).GetList()
    return file_list

def list_folders(drive, parent, subfoldername=None):
    if parent is None:
        parent_id = "root"
    else:
        parent_id = parent['id']

    q_str = "'{}' in parents and mimeType contains 'application/vnd.google-apps.folder' and trashed=false".format(parent_id)

    if subfoldername is not None:
        q_str += "and title = '{}'".format(subfoldername)
    file_list = drive.ListFile({'q': q_str}).GetList()
    return file_list

def create_subfolder(drive, parent, subfoldername):
    new_folder = drive.CreateFile({'title':'{}'.format(subfoldername),
                                   'mimeType':'application/vnd.google-apps.folder',
                                   })
    if parent is not None:
        new_folder['parents'] = [{u'id': parent['id']}]
    new_folder.Upload()
    return new_folder

def ensure_subfolder(drive, parent, subfoldername):
    l = list_folders(drive, parent, subfoldername=subfoldername)
    if l:
        return l[0]
    else:
        new_folder = create_subfolder(drive, parent, subfoldername)
        return new_folder


def get_trimester_year_num(utdate_year, utdate_month):
    trimester_year = utdate_year
    if utdate_month == 12:
        trimester_year += 1
        trimester_num = 1
    elif utdate_month <= 3:
        trimester_num = 1
    elif utdate_month <= 7:
        trimester_num = 2
    else:
        trimester_num = 3

    return trimester_year, trimester_num


def get_trimester_name(utdate_tuple):
    trimester_year, trimester_num = get_trimester_year_num(utdate_tuple[0],
                                                           utdate_tuple[1])
    trimester_name = "%04dT%d" % (trimester_year, trimester_num)

    return trimester_name

def get_utdate_string(utdate_tuple):
    utdate = "%04d%02d%02d" % utdate_tuple
    return utdate
