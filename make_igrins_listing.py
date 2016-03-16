from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

from gdrive_helper import get_trimester_name, list_files, list_folders, get_utdate_string

def get_archive_listing(drive, utdate_tuple):
    timester_name = get_trimester_name(utdate_tuple)
    utdate = get_utdate_string(utdate_tuple)

    chdir = "igrins_data"
    igrins_data = list_folders(drive, None, chdir)
    if not igrins_data:
        raise RuntimeError("no directory with of given date is found: %s" % chdir)

    chdir = timester_name
    parent = list_folders(drive, igrins_data[0], chdir)
    if not parent:
        raise RuntimeError("no directory with of given date is found: %s" % chdir)

    chdir = utdate
    folder = list_folders(drive, parent[0], chdir)
    if not folder:
        raise RuntimeError("no directory with of given date is found: %s" % chdir)


    l = list_files(drive, folder[0])
    ll = [(l1["title"], l1) for l1 in l]
    ll.sort()

    link_format = "https://drive.google.com/uc?export=download&id={}"

    listing = []
    for fn, l1 in ll:
        link = link_format.format(l1["id"])
        listing.append((fn, link, l1['md5Checksum']))
    return listing

def write_listing(utdate_tuple):

    gauth = GoogleAuth()
    #gauth.CommandLineAuth()
    gauth.LocalWebserverAuth()

    drive = GoogleDrive(gauth)

    listing = get_archive_listing(drive, utdate_tuple)
    outlines = ["{} {} {}\n".format(*r) for r in listing]
    utdate = get_utdate_string(utdate_tuple)
    header = ["# IGRINS Listing for %s\n" % utdate]
    outname = "igrins_{}.list".format(utdate)
    open("igrins_{}.list".format(utdate), "w").writelines(header+outlines)
    print "%s is written" % outname

if __name__ == "__main__":
    import sys
    if len(sys.argv) not in [4]:
        print "execname year month day"
        sys.exit(0)

    utdate_tuple = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])

    write_listing(utdate_tuple)
