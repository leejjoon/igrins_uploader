import re
from pydrive.drive import GoogleDrive


from gdrive_helper import (get_trimester_name, list_files, 
                           list_folders, get_obsdate_string,
                           authorize)

def get_archive_listing(drive, obsdate_tuple):
    timester_name = get_trimester_name(obsdate_tuple)
    obsdate = get_obsdate_string(obsdate_tuple)

    chdir = "igrins_data"
    igrins_data = list_folders(drive, None, chdir)
    if not igrins_data:
        raise RuntimeError("no directory with of given date is found: %s" % chdir)

    chdir = timester_name
    parent = list_folders(drive, igrins_data[0], chdir)
    if not parent:
        raise RuntimeError("no directory with of given date is found: %s" % chdir)

    chdir = obsdate
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


def write_listing(obsdate_tuple, outname_template, filename_filter):

    credfile = "igrins_upload_cred.txt"

    gauth = authorize(credfile)

    drive = GoogleDrive(gauth)

    listing = get_archive_listing(drive, obsdate_tuple)

    if filename_filter:
        p = re.compile(filename_filter)
        outlines = ["{} {} {}\n".format(*r) for r in listing
                    if p.match(r[0])]
    else:
        outlines = ["{} {} {}\n".format(*r) for r in listing]

    obsdate = get_obsdate_string(obsdate_tuple)
    header = ["# IGRINS Listing for %s\n" % obsdate]
    # outname = "igrins_{}.list".format(obsdate)
    outname = outname_template.format(obsdate=obsdate)
    open(outname, "w").writelines(header+outlines)
    print("%s is written" % outname)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Download gdrive list.')
    parser.add_argument('--outname',
                        default="igrins_{obsdate}.list",
                        help='outname')
    parser.add_argument('--filter',
                        default="",
                        help='regext for filtering filename')
    parser.add_argument('year', type=int,
                        help='year')
    parser.add_argument('month', type=int,
                        help='month')
    parser.add_argument('day', type=int,
                        help='day')

    args = parser.parse_args()

    # obsdate_tuple = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
    obsdate_tuple = args.year, args.month, args.day

    write_listing(obsdate_tuple, args.outname,
                  filename_filter=args.filter)
