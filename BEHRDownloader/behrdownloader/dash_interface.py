#!/usr/bin/env python
from __future__ import print_function
import argparse
import datetime as dt
import os
import requests
import tarfile

from .utils import smart_open

import pdb

dash_root = "https://dash.ucop.edu"
request_params = {"accept": "application/json"}
default_block_size_bytes = 4096

behr_dois = {'daily-gridded': 'doi:10.6078/D12D5X',
             'monthly-gridded': 'doi:10.6078/D1RQ3G',
             'daily-native': 'doi:10.6078/D1WH41',
             'monthly-native': 'doi:10.6078/D1N086'}

"""
This uses the Dash REST API (https://dash.ucop.edu/api/docs/index.html and 
https://github.com/CDLUC3/stash/blob/development/stash_api/basic_submission.md) 
to download files from the above BEHR repositories.
"""


def replace_ascii_html(s):
    """
    Replace certain ASCII characters with their HTML code equivalent

    Currently only ':' and '/' are replaced.

    :param s: the string to do the replacement in
    :type s: str

    :return: str with the replaced characters.
    """
    html_table = {':': '%3A',
                  '/': '%2F'}
    for k, v in html_table.items():
        s = s.replace(k, v)

    return s


def get_dash_files_from_doi(doi):
    """
    Create a dictionary linking file names to URLs for the dataset pointed to by a DOI.

    :param doi: the DOI as a string starting with "doi:"
    :type doi: str

    :return: a dictionary with file names as the keys and URLs as the values.
    """
    doi = replace_ascii_html(doi)
    # First, we get a list of all versions associated with this DOI
    versions = requests.get("{}/api/datasets/{}/versions".format(dash_root, doi), params=request_params)

    # Find the most recent version
    newest_version = -1
    newest_idx = -1
    for idx, a_version in enumerate(versions.json()['_embedded']['stash:versions']):
        if a_version['versionNumber'] > newest_version:
            newest_version = a_version['versionNumber']
            newest_idx = idx

    if newest_idx < 0:
        raise RuntimeError('Failed to find the newest version')


    # Assuming that the list of versions is in chronological order, we want the most recent one
    # In that version get the URL to request the first page of files
    file_url = versions.json()['_embedded']['stash:versions'][newest_idx]['_links']['stash:files']['href']

    file_dict = dict()

    while True:
        file_group = requests.get("{}{}".format(dash_root, file_url), params=request_params).json()
        # Now we can retrieve a list of the available files
        file_list = file_group['_embedded']['stash:files']

        # Extract the file name and link into a more easily comprehendable dict
        file_dict.update({f['path']: dash_root + f['_links']['stash:download']['href'] for f in file_list})

        # The files aren't all returned at once - 10 are listed per "page" so as long as there is a next page, we need
        # to get the files listed on that page and add them to the dictionary
        if 'next' in file_group['_links'].keys():
            file_url = file_group['_links']['next']['href']
        else:
            break

    return file_dict


def download_file(url, out_name, block_size=default_block_size_bytes):
    """
    Download a file from the given URL.

    :param url: the URL to download
    :type url: str

    :param out_name: the name to give the downloaded file
    :type out_name: str

    :param block_size: the size in bytes to download at once. This function will iterate until the whole file is
        downloaded. Optional, default is 4096
    :type block_size: int

    :return: None
    """
    # Requesting the URL as a stream will not try to download the entire file at once
    dl_obj = requests.get(url, stream=True)
    dl_obj.raise_for_status()

    with open(out_name, 'wb') as outfile:
        for block in dl_obj.iter_content(block_size):
            outfile.write(block)


def extract_tar_file(filename, delete_tar=False, verbose=0, logging_fxn=print):
    """
    Extract individual files from a gzipped tar archive.

    :param filename: the file name of the .tgz file to unarchive
    :type filename: str

    :param delete_tar: optional, determines whether to delete the .tgz file after unarchiving. Default is ``False``.
    :type delete_tar: bool

    :param verbose: Controls the logging verbosity. Default is 0
    :type verbose: int

    :param logging_fxn: optional, the function to call to print logging messages. Default is ``print``
    :type logging_fxn: function

    :return: None
    """
    extract_path = os.path.dirname(filename)
    with tarfile.open(filename, 'r:gz') as tarobj:
        def is_within_directory(directory, target):
            
            abs_directory = os.path.abspath(directory)
            abs_target = os.path.abspath(target)
        
            prefix = os.path.commonprefix([abs_directory, abs_target])
            
            return prefix == abs_directory
        
        def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
        
            for member in tar.getmembers():
                member_path = os.path.join(path, member.name)
                if not is_within_directory(path, member_path):
                    raise Exception("Attempted Path Traversal in Tar File")
        
            tar.extractall(path, members, numeric_owner=numeric_owner) 
            
        
        safe_extract(tarobj, path=extract_path)
    if delete_tar:
        if verbose > 0:
            logging_fxn('Deleting {}'.format(filename))
        os.remove(filename)


def iter_months(start, end):
    """
    Iterate over months between a start and end date

    :param start: the first month's file to include
    :type start: datetime.datetime

    :param end: the last month's file to include
    :type end: datetime.datetime

    :return: iterable with datetime objects on the first of each month
    """
    curr_date = start.replace(day=1)
    while curr_date <= end:
        yield curr_date
        curr_date += dt.timedelta(days=32)
        curr_date = curr_date.replace(day=1)


def iter_files_for_dates(filenames, start, end):
    """
    Iterate over BEHR files between the given start and end dates.

    :param filenames: the dictionary of BEHR monthly tar files mapped to their DASH URLs, generated by
        :func:`get_dash_files_from_doi`.
    :type file_dict: dict

    :param start: the first month's file to include
    :type start: datetime.datetime

    :param end: the last month's file to include
    :type end: datetime.datetime

    :return: iterates returning file name and URL pairs.
    """
    for date in iter_months(start, end):
        date_string = date.strftime('%Y%m')
        for f in filenames.keys():
            if date_string in f:
                yield f, filenames[f]
                break  # break the inner loop, assume that there's only one file per month


def download_and_extract(file_dict, start, end, out_dir='.', extract_tar=False, delete_tar=False, logging_fxn=print, verbose=0, **kwargs):
    """
    Automatically download, and optionally extract, BEHR monthly .tar archives

    :param file_dict: the dictionary of BEHR monthly tar files mapped to their DASH URLs, generated by
        :func:`get_dash_files_from_doi`.
    :type file_dict: dict

    :param start: the first month of BEHR data to download
    :type start: datetime.datetime

    :param end: the last month of BEHR data to download
    :type end: datetime.datetime

    :param out_dir: the directory to save the files to. Default is ``"."``, i.e. the current directory.
    :type out_dir: str

    :param extract_tar: optional, determines whether or not to automatically extract the individual .hdf files from the
        .tgz archive. Default is ``False``.
    :type extract_tar: bool

    :param delete_tar: optional, determines whether to delete the .tgz files after extracting the .hdf files. Has no
        effect if ``extract_tar`` is ``False``. Default is ``False``.
    :type delete_tar: bool

    :param logging_fxn: optional, the function to call to print logging messages. Default is ``print``
    :type logging_fxn: function

    :param verbose: Controls the logging verbosity. Default is 0
    :type verbose: int

    :param kwargs: unused, present to consume extra command link arguments passed through.

    :return: None

    Note: if called from :func:`driver` then ``file_dict``, ``start``, and ``end`` are automatically passed, only
    the other keyword arguments need be added to the call to driver if desired. E.g., if you wanted to download
    BEHR daily profile gridded files for 2005, and extract the archives automatically, call::

        driver('daily-gridded', datetime(2005,1,1), datetime(2005,12,1), 'download', extract_tar=True)

    (assuming that you had done ``from datetime import datetime``).
    """
    if not os.path.isdir(out_dir):
        raise ValueError('outdir must be an existing directory')

    for fname, url in iter_files_for_dates(file_dict, start, end):
        save_name = os.path.join(out_dir, fname)
        if verbose > 0:
            logging_fxn('Saving {} as {}'.format(url, save_name))
        download_file(url, save_name)
        if extract_tar:
            if verbose > 0:
                logging_fxn('Extracting {}'.format(save_name))
            extract_tar_file(save_name, delete_tar=delete_tar, verbose=verbose, logging_fxn=logging_fxn)


def list_files(file_dict, start, end, out_file=None, link_format='raw', **kwargs):
    """
    Return a list of download links for BEHR files. Optionally, write the list to the screen or a file.

    :param file_dict: the dictionary of BEHR monthly tar files mapped to their DASH URLs, generated by
        :func:`get_dash_files_from_doi`.
    :type file_dict: dict

    :param start: the first month of BEHR data to download
    :type start: datetime.datetime

    :param end: the last month of BEHR data to download
    :type end: datetime.datetime

    :param out_file: the output file to write the list of links to. Default is None, i.e. do not write them (just
        return the list). A value of ``"-"`` will write to stdout (i.e. the screen). Any other string that specifies
        a valid filename will cause this to write to that file.
    :type out_file: None or str

    :param link_format: How to format the links. Default is ``"raw"``, which just prints the URLs by themselves. Other
        options are ``"unix"`` which prints a list of wget commands that can be executed as a unix shell script, and
        ``"powershell"``, which prints a list of Powershell commands for use on Windows.
    :type link_format: str

    :param kwargs: unused, present to consume extra command link arguments passed through.

    :return: a list of links
    :rtype: list of str

    Note: if called from :func:`driver` then ``file_dict``, ``start``, and ``end`` are automatically passed, only
    the other keyword arguments need be added to the call to driver if desired. E.g., if you wanted to get a list
    of Unix-style commands for daily profile gridded files for 2005, call::

        driver('daily-gridded', datetime(2005,1,1), datetime(2005,12,1), 'list', link_format='unix')

    (assuming that you had done ``from datetime import datetime``).
    """
    links = []
    for fname, url in iter_files_for_dates(file_dict, start, end):
        if link_format.lower() == 'raw':
            links.append('{}\n'.format(url))
        elif link_format.lower() == 'unix':
            links.append('wget -O {} {}\n'.format(fname, url))
        elif link_format.lower() == 'powershell':
            links.append('Invoke-WebRequest {} -OutFile {}\n'.format(url, fname))
        else:
            raise ValueError('The file format "{}" is not recognized'.format(link_format))

    if out_file is not None:
        with smart_open(out_file) as fobj:
            fobj.write(''.join(links))

    return links


def driver(dataset, start, end, action, verbose=0, **kwargs):
    """
    Main function to download or get links for BEHR files for a given date range.

    This function is meant to be called either directory or as a result of launching this program from the command line,
    so accepts the command line arguments.

    :param dataset: which dataset (gridded or native, daily or monthly profiles) to download. This must be either a
        dataset name defined in the behr_dois dictionary or a DOI as string starting with "doi:"
    :type dataset: str

    :param start: the first month of BEHR data to retrieve
    :type start: datetime.datetime

    :param end: the last month of BEHR data to retrieve
    :type end: datetime.datetime

    :param action: controls whether to list the download links ("list") or directly download the files ("download")
    :type action: str

    :param verbose: Controls the logging verbosity. Default is 0
    :type verbose: int

    :param kwargs: Additional keyword arguments accepted, either from the command line parsing or in a direct call.
        These are passed to the :func:`list_files` function if ``action`` is ``"list"`` or :func:`download_and_extract`
        function if ``action`` is ``"download"``; see those files' documentation for additional keyword arguments
        accepted/required.

    :return: return value of :func:`list_files` if ``action`` is ``"list"`` or :func:`download_and_extract` if ``action``
        is ``"download"``
    """
    if not dataset.startswith('doi'):
        try:
            dataset = behr_dois[dataset]
        except KeyError:
            raise ValueError('dataset must be a DOI string beginning with "doi" or one of the following strings: {}'.format(
                ', '.join(behr_dois.keys())
            ))

    file_dict = get_dash_files_from_doi(dataset)

    if action.lower() == 'download':
        return download_and_extract(file_dict=file_dict, start=start, end=end, verbose=verbose, **kwargs)
    elif action.lower() == 'list':
        return list_files(file_dict=file_dict, start=start, end=end, **kwargs)


def parse_cl_date(date_string):
    return dt.datetime.strptime(date_string, '%Y-%m')


def parse_args(parser=None):
    """
    Parse command line arguments, or add the arguments to a given parser.

    :param parser: optional, default is None. A parser to add the command line arguments to. If one is not given,
        it will be created. Giving a parser as an input is intended to allow these arguments to be added to a subparser
        used in a call to a master program.
    :type parser: None or ``argparse.ArgumentParser``

    :return: the parsed arguments namespace if no parser is given, otherwise None.
    """
    called_as_subcommand = parser is not None
    description = 'Interface to batch download BEHR files from University of California DASH'
    epilog = 'Example: {} daily-gridded 2005-01 2005-02'.format(os.path.basename(__file__))
    if not called_as_subcommand:
        parser = argparse.ArgumentParser(description=description, epilog=epilog)

    parser.add_argument('action', choices=['list', 'download'], help='What action to take. "list" will ')
    parser.add_argument('dataset', choices=behr_dois.keys(), help='Which dataset to download.')
    parser.add_argument('start', type=parse_cl_date, help='Beginning date to download in yyyy-mm format.')
    parser.add_argument('end', type=parse_cl_date, help='Ending date to download in yyyy-mm format.')
    parser.add_argument('-v', '--verbose', action='count', help='Increase logging to console.')

    list_args = parser.add_argument_group(title='List', description='Arguments specific to the "list" action')
    list_args.add_argument('-f', '--out-file', default='-', help='File to save the URLs to. By default, they are just printed to stdout.')
    list_args.add_argument('--link-format', choices=['raw', 'unix', 'powershell'], default='raw',
                           help='What format to create the list of links in. Default is "raw", which prints each '
                                'link on its own line. Other options are "unix", which generates a list of wget commands '
                                'that can be executed as a Bash script, and "powershell", which creates a list of download '
                                'commands that can be executed as a PowerShell script.')

    download_args = parser.add_argument_group(title='Download', description='Arguments specific to the "download" action')
    download_args.add_argument('-o', '--out-dir', default='.', help='Directory to save downloads to. Default is the current directory.')
    download_args.add_argument('-e', '--extract-tar', action='store_true', help='Extract the tar files after downloading')
    download_args.add_argument('-d', '--delete-tar', action='store_true', help='Delete tar file after extracting. Has no effect without --extract-tar.')

    parser.set_defaults(driver_fxn=driver)

    if not called_as_subcommand:
        return parser.parse_args()


def main(subparser=None):
    args = parse_args(subparser)
    driver(**vars(args))


if __name__ == '__main__':
    main()