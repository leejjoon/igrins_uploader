import sys
import time
import curio
from curio_timeout_restart import timeout_restart

from pydrive.drive import GoogleDrive
from gdrive_helper import (get_trimester_name,
                           list_files,
                           ensure_subfolder,
                           get_obsdate_string,
                           authorize)


def get_upload_file_list(obsdate_tuple, indata_format):

    obsdate = get_obsdate_string(obsdate_tuple)

    trimester_name = get_trimester_name(obsdate_tuple)

    import glob
    import os.path

    indata_dir = indata_format.format(obsdate=obsdate,
                                      trimester=trimester_name)
    fn_list_hk = glob.glob(os.path.join(indata_dir, "SDC[HK]*.fits"))
    fn_list_hk.sort()
    fn_list_log = glob.glob(os.path.join(indata_dir, "*.txt"))
    fn_list_log.sort()
    fn_list_s = glob.glob(os.path.join(indata_dir, "SDCS*.fits.fz"))
    fn_list_s.sort()
    fn_list = fn_list_log + fn_list_hk + fn_list_s

    if not fn_list:
        raise RuntimeError("no file to upload for %s" % obsdate)

    return fn_list


def upload_google_drive(obsdate_tuple, indata_format, dry_run,
                        tick_callback=None):

    credfile = "igrins_upload_cred.txt"

    gauth = authorize(credfile)

    drive = GoogleDrive(gauth)

    obsdate = "%04d%02d%02d" % obsdate_tuple

    fn_list = get_upload_file_list(obsdate_tuple, indata_format)

    trimester_name = get_trimester_name(obsdate_tuple)
    igrins_data = ensure_subfolder(drive, None, "igrins_data")
    parent = ensure_subfolder(drive, igrins_data, trimester_name)

    folder = ensure_subfolder(drive, parent, obsdate)

    l = list_files(drive, folder)
    existing_file_names = [l1["title"] for l1 in l]

    import os.path
    for fn in fn_list:
        fn0 = os.path.split(fn)[-1]
        if fn0 in existing_file_names:
            print("skipping {}".format(fn0))
            continue

        kw = {'title': fn0,
              'parents': [folder],
              }

        if fn0.endswith("fits"):
            kw["mimeType"] = 'image/fits'

        if not dry_run:
            retry_count = 5
            starttime = time.time()
            while True:
                if retry_count < 0:
                    sys.exit(1)
                try:
                    f = drive.CreateFile(kw)
                    f.SetContentFile(fn)
                    f.Upload()
                    print('uploaded file %s with mimeType %s' % (f['title'], f['mimeType']))
                    print("Time elapsed: %.1f" % (time.time() - starttime,) )
                    if tick_callback is not None:
                        tick_callback()
                    break
                except KeyboardInterrupt:
                    raise
                except:
                    print("Error :", retry_count)
                    retry_count -= 1
        else:
            print('will upload file %s' % (fn0,))


# Created file hello.png with mimeType image/png

if __name__ == "__main__":
    import os
    import argparse

    parser = argparse.ArgumentParser(description='Archive IGRINS data to google drive.')
    parser.add_argument('--rootdir', "-r",
                        default=".",
                        help='rootdir where the fits files are found')
    parser.add_argument('--childdir-format', "-c",
                        default="{trimester}/{obsdate}",
                        help='rootdir where the fits files are found')

    parser.add_argument('--dry', dest='dry', action='store_true')
    parser.add_argument('--no-dry', dest='dry', action='store_false')
    parser.add_argument('--timeout-second', default=0., type=float)
    parser.set_defaults(dry=True)

    parser.add_argument('year', type=int,
                        help='year')
    parser.add_argument('month', type=int,
                        help='month')
    parser.add_argument('day', type=int,
                        help='day')

    args = parser.parse_args()
    print(args.rootdir, args.childdir_format, args.dry, args.year)

    obsdate_tuple = (args.year, args.month, args.day)
    indata_format = os.path.join(args.rootdir, args.childdir_format)

    if args.timeout_second > 0.:
        address = ("localhost", 3001)
        curio.run(timeout_restart(address, args.timeout_second,
                                  upload_google_drive, obsdate_tuple,
                                  indata_format, args.dry))
    else:
        upload_google_drive(obsdate_tuple, indata_format,
                            dry_run=args.dry)

