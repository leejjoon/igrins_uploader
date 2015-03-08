from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


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


def get_upload_file_list(utdate_tuple, indata_format):

    utdate = "%04d%02d%02d" % utdate_tuple

    trimester_name = get_trimester_name(utdate_tuple)

    #print utdate, trimester_name
    #raw_input()

    import glob
    import os.path

    indata_dir = indata_format.format(utdate=utdate,
                                      trimester=trimester_name)
    fn_list1 = glob.glob(os.path.join(indata_dir, "*.fits"))
    fn_list2 = glob.glob(os.path.join(indata_dir, "*.txt"))
    fn_list = fn_list1 + fn_list2
    fn_list.sort()

    if not fn_list:
        raise RuntimeError("no file to upload for %s" % utdate)


    return fn_list

def upload_google_drive(utdate_tuple, indata_format, dry_run):
    gauth = GoogleAuth()
    #gauth.LocalWebserverAuth()
    gauth.CommandLineAuth()

    drive = GoogleDrive(gauth)

    utdate = "%04d%02d%02d" % utdate_tuple

    fn_list = get_upload_file_list(utdate_tuple, indata_format)

    trimester_name = get_trimester_name(utdate_tuple)
    igrins_data = ensure_subfolder(drive, None, "igrins_data")
    parent = ensure_subfolder(drive, igrins_data, trimester_name)


    folder = ensure_subfolder(drive, parent, utdate)

    l = list_files(drive, folder)
    existing_file_names = [l1["title"] for l1 in l]

    import os.path
    for fn in fn_list:
        fn0 = os.path.split(fn)[-1]
        if fn0 in existing_file_names:
            print "skipping {}".format(fn0)
            continue


        kw = {'title': fn0,
              'parents': [folder],
              }

        if fn0.endswith("fits"):
            kw["mimeType"] = 'image/fits'

        if not dry_run:
            f = drive.CreateFile(kw)
            f.SetContentFile(fn)
            f.Upload()
            print 'uploaded file %s with mimeType %s' % (f['title'], f['mimeType'])
        else:
            print 'will uploaded file %s' % (fn0,)


# Created file hello.png with mimeType image/png

if __name__ == "__main__":
    import sys
    if len(sys.argv) not in [4, 5]:
        print "execname year month day"
        sys.exit(0)

    if len(sys.argv) == 5 and sys.argv[-1] == "upload":
        dry_run = False
    else:
        dry_run = True
    utdate_tuple = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
    indata_format = "/Users/igrins_data/IGRINS_DATA_0/data/{trimester}/{utdate}"
    upload_google_drive(utdate_tuple, indata_format, dry_run)
