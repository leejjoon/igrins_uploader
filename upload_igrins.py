from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

from gdrive_helper import (get_trimester_name,
                           list_files,
                           ensure_subfolder,
                           get_utdate_string)

def get_upload_file_list(utdate_tuple, indata_format):

    utdate = get_utdate_string(utdate_tuple)

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
