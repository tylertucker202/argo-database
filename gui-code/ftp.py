
"""Utilities for handle Argo FTP mirrors
"""

from datetime import datetime
import os.path
import re
from ftplib import FTP
import tempfile

import argo


def parse_listedfile(parts):
    """
       Parse a text line like:
       -rw-r--r--    1 ftp      ftp         17336 Aug 20  2016 R13857_136.nc
    """
    data = {'filename': parts[-1], 'size': parts[4]}
    if ":" in parts[7]:
        d = " ".join([str(datetime.now().year)] + parts[5:8])
        rule = "%Y %b %d %H:%M"
    else:
        d = " ".join(parts[5:8])
        rule = "%b %d %Y"
    data['datetime'] = datetime.strptime(d, rule)
    return data


def list_contents(conn, top='/'):
    conn.cwd(top) 
    content = []
    conn.retrlines('LIST', content.append)
    dirs = []
    files = []
    for item in (x.split() for x in content):
        if item[0][0] == 'd':
            dirs.append(item[-1])
        else:
            tmp = parse_listedfile(item)
            tmp['path'] = top
            tmp['host'] = conn.host
            files.append(tmp)
    return top, dirs, files        


def find_ftp_files(conn, top='/', pattern='^[DR]\d+_\d+D?.nc'):
    """Walk throught an FTP server searching to a pattern of files

       conn: an FTP connection
       top: Search this path and anything inside it
       pattern: Return the files that match this pattern
    """
    assert top[0] == '/', 'Must be an absolute path, starting with "/"'
    root, dirs, files = list_contents(conn, top)
    for f in files:
        if re.search(pattern, f['filename']):
            yield f
    for d in dirs:
        for f in find_ftp_files(conn, os.path.join(root, d), pattern):
            if re.search(pattern, f['filename']):
                yield f


def profiles_from_ftp(conn, filename):
    """Create an Argo profile object from a remote FTP file
    """
    conn.cwd(os.path.dirname(filename))
    with tempfile.NamedTemporaryFile(mode='w+b', dir=None, delete=True) as tmp:
        conn.retrbinary('RETR %s' % os.path.basename(filename), tmp.write)
        tmp.file.flush()
        profiles = argo.get_profiles_from_nc(tmp.name)
    return profiles
