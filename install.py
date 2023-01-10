#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0321, W0621

"""
Install Database Script.

~~~~~~~~~~~~~~~~~~~~~~~
Install SQLite database to a specific folder

version : v0.1
Author  : Ismail SEZEN sezenismail@gmail.com
license : AGPLv3
date    : 2021-02-17
"""

import os
import pickle
import sqlite3 as sq3
import sys

from glob import glob
from os import makedirs
from os import path
from shutil import copyfile as copyf
import tempfile
from timeit import default_timer as timer

import argparse as _argparse

from argparse import RawTextHelpFormatter as _rtformatter
from contextlib import closing

__prog__ = 'Install database'
__version__ = 'v0.2'
__author__ = 'Ismail SEZEN'
__email__ = 'sezenismail@gmail.com'
__github__ = 'isezen'
__license__ = 'AGPLv3'
__year__ = '2021'


def _create_argparser_(desc, epilog):
    """Create an argparser object."""
    file_py = path.basename(sys.argv[0])
    p = _argparse.ArgumentParser(
        description=desc, epilog=epilog.format(file_py),
        formatter_class=_rtformatter)
    vstr = '{} {}\n{} (c) {} {}'
    p.add_argument(
        '-v', '--version', help="version", action="version",
        version=vstr.format(__prog__, __version__,
                            __license__, __year__,
                            __author__))
    return p


def read_pkl(file):
    """Read pickle file."""
    with open(file, 'rb') as f:
        x = pickle.load(f)
    return x


def get_meta_id_list(cur):
    """Get meta table as list."""
    cur.execute("SELECT id, name FROM meta")
    rows = cur.fetchall()
    return list(map(list, zip(*rows)))


def get_meta_id(meta_name, meta_list):
    """Get meta id from database."""
    return [meta_list[0][meta_list[1].index(v)] if v in meta_list[1] else 0
            for v in meta_name]


def get_ids(file):
    """Get parameter and station ids from file name."""
    bn = path.basename(file).split('.')[0]
    return (int(i) for i in bn.split('_'))


def split2meta(v):
    """Split a list to values and meta."""
    m = [i if isinstance(i, str) else 'ok' for i in v]
    v = [i if isinstance(i, float) else float('NaN') for i in v]
    return m, v


def pbar(  # pylint: disable=R0913
    iterable, prefix='', suffix='',
    decimals=1, length=100, fill='█',
    printEnd="\r"
):
    """Call in a loop to create terminal progress bar."""
    total = len(iterable)

    def print_pbar(iteration):
        percent = ("{0:." + str(decimals) + "f}").format(
            100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        br = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{br}| {percent}% {suffix}', end=printEnd)

    print_pbar(0)
    for i, item in enumerate(iterable):
        yield item
        print_pbar(i + 1)
    print()


def get_measurement_values(pth):
    """Get measured parameter values from pkl files."""
    pkl_files = glob(path.join(pth, '*.pkl'))
    pkl_files.remove(path.join(pth, 'index.pkl'))
    pkl_files.sort()
    for file in pbar(pkl_files,
                     prefix='Creating Measurement Table', length=50):
        pol_id, sta_id = get_ids(file)
        yield pol_id, sta_id, True


def get_values(pth):
    """Get a row from pickles."""
    index_pkl = path.join(pth, 'index.pkl')
    pkl_files = glob(path.join(pth, '*.pkl'))
    pkl_files.remove(index_pkl)
    pkl_files.sort()
    dates = read_pkl(index_pkl)
    for file in pbar(pkl_files, prefix='Building Data', length=50):
        v = read_pkl(file)
        pol_id, sta_id = get_ids(file)
        for i, d in zip(v, dates):
            if not isinstance(i, str):
                yield pol_id, sta_id, d, i, 0


def get_meta(path, meta_list):
    """Get a row from pickles."""
    index_pkl = path.join(path, 'index.pkl')
    pkl_files = glob(path.join(path, '*.pkl'))
    pkl_files.remove(index_pkl)
    pkl_files.sort()
    dates = read_pkl(index_pkl)
    for file in pbar(pkl_files, prefix='Creating Meta', length=50):
        v = read_pkl(file)
        pol_id, sta_id = get_ids(file)
        m = [i for i, v in enumerate(v) if isinstance(v, str)]
        date = [dates[i] for i in m]
        m = get_meta_id([v[i] for i in m], meta_list)
        for j, d in zip(m, date):
            yield pol_id, sta_id, d, j


def agree_to_lic():
    """Accept or decline licence agreement."""
    print(get_licence())
    a = input('\nDo you accept? (yes or [No]) ')
    return a.lower() == 'yes' or a.lower() == 'y'


def target_is_writable(pth):
    """Check if target directory is writable."""
    if pth == '':
        pth = os.getcwd()
    if path.exists(pth):
        if not os.access(pth, os.W_OK):
            raise PermissionError(
                f"You don't have permission to write to '{pth}'.")
    else:
        udir = path.split(pth)[0]
        return target_is_writable(udir)
    return True


def create_tmp_copy(pth):
    """Create temporary copy of the file."""
    fn_tmp = next(tempfile._get_candidate_names())  # pylint: disable=W0212
    fn_tmp = path.join(tempfile.gettempdir(),
                       path.basename(pth) + '_' + fn_tmp)
    copyf(pth, fn_tmp)
    return fn_tmp


def get_cwd():
    """Get dir name of path to this file."""
    cwd = path.dirname(__file__)
    if cwd == '':
        cwd = '.'
    if not os.access(cwd, os.R_OK):
        raise PermissionError("You don't have permission to read from '" +
                              cwd + "'.")
    return cwd


def get_paths():
    """Get required paths to db and pkl folders."""
    cwd = get_cwd()
    db_files = glob(path.join(cwd, '*.db'))
    lic_file = glob(path.join(cwd, '*.LICENSE'))
    if len(db_files) < 1:
        raise FileNotFoundError('Database cannot be found')
    return db_files[0], path.join(cwd, 'pkl'), lic_file[0]


def get_licence():
    """Get licence text for database."""
    file_db, _, _ = get_paths()
    file_db_name = path.basename(file_db)
    fn, _ = path.splitext(file_db_name)
    lic_file = path.join(get_cwd(), fn + ".LICENSE")
    if not path.exists(lic_file):
        raise FileExistsError(path.basename(lic_file) + ' does not exist')
    return open(lic_file, "r", encoding='utf8').read()


def install(where):
    """Install database."""
    target_is_writable(where)  # check write permission
    file_db, path_to_pkl, lic_file = get_paths()
    tmp = create_tmp_copy(file_db)
    with closing(sq3.connect(tmp)) as con:
        with closing(con.cursor()) as cur:
            rows = get_values(path_to_pkl)
            cur.executemany('INSERT INTO data VALUES(?,?,?,?,?);', rows)
            con.commit()
            #
            rows = get_measurement_values(path_to_pkl)
            cur.executemany('INSERT INTO measurement VALUES(?,?,?);', rows)
            con.commit()

        print('Creating indices...')
        par = {'param': 'param, sta, date', 'date': 'date', 'sta': 'sta'}
        with closing(con.cursor()) as cur:
            for k, v in par.items():
                cur.execute(f'CREATE INDEX {k}_index ON data ({v})')
    makedirs(where, exist_ok=True)
    copyf(tmp, path.join(where, path.basename(file_db)))
    os.remove(tmp)
    copyf(lic_file, path.join(where, path.basename(lic_file)))


if __name__ == '__main__':
    description = 'Install database.\n' + \
                  'WARNING: Make sure you have the correct permission\n' + \
                  'to install database to PATH'
    epilog = 'Example of use:\n' + \
             ' %(prog)s /usr/local/share\n' + \
             ' %(prog)s C:\\air-database\n'

    try:
        p = _create_argparser_(description, epilog)
        p.add_argument('PATH', type=str, help='Path to install database')
        p.add_argument('-l', '--license', help="print database license",
                       action="version", version=get_licence())
        args = p.parse_args()
        target_is_writable(args.PATH)
        if agree_to_lic():
            s = timer()
            install(args.PATH)
            elapsed = timer() - s
            print('Database created in', int(elapsed // 60), 'min.',
                  int(elapsed % 60), 'sec.', '\nSUCCESS!')
    except Exception as e:  # pylint: disable=W0703
        print(e)
