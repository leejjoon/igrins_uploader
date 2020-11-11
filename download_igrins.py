import re
from pydrive.drive import GoogleDrive
from get_hash import getHash

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

    # listing = []
    # for fn, l1 in ll:
    #     link = link_format.format(l1["id"])
    #     listing.append((fn, link, l1['md5Checksum']))
    # return listing

    return ll

if False:
    obsdate_tuple = (2020, 11, 2)

def write_files(obsdate_tuple, outdir_template, filename_filter):

    credfile = "igrins_upload_cred.txt"

    gauth = authorize(credfile)

    drive = GoogleDrive(gauth)

    listing = get_archive_listing(drive, obsdate_tuple)

    if filename_filter:
        p = re.compile(filename_filter)
        listing = [r for r in listing if p.match(r[0])]

    obsdate = get_obsdate_string(obsdate_tuple)
    outdir = outdir_template.format(obsdate=obsdate)

    import pathlib
    op = pathlib.Path(outdir)
    op.mkdir(parents=True, exist_ok=True)

    for fn, gdrive_file in listing:
        outfile = op.joinpath(fn)
        outname = outfile.absolute()
        if outfile.exists():
            hash = getHash(outname)
            if hash == gdrive_file['md5Checksum']:
                print("skip..", fn)
                continue
        print("downloadfile..", fn)
        gdrive_file.GetContentFile(outname)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Download gdrive files.')
    parser.add_argument('--outdir',
                        default="{obsdate}",
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

    write_files(obsdate_tuple, args.outdir,
                filename_filter=args.filter)
