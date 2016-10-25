from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from __builtin__ import int

import h5py
import os
import re
import sys
import pdb

def print_usage():
    print("Usage: python {0} <files>".format(sys.argv[0]))
    print("    Splits OMI_BEHR .hdf (version 5) files into individual swaths")
    print("    Pass the files to split as arguments to this program.")
    print("    Examples:")
    print("        python {0} OMI_BEHR_v2-1B_20150601.hdf")
    print("        (this will split the file OMI_BEHR_v2-1B_20150601.hdf into")
    print("         files with -SwathNNNNN appended after the date.)")
    print("")
    print("        python {0} OMI_BEHR_v2-1B_201506*")
    print("        (this will split all files from June.")
    print("")
    print("    Output files are saved to the same directory as the input files.")
    print("    Files to split must begin with OMI_BEHR and end in .hdf.")
    exit(0)

def parse_args(args):
    if len(args) < 2 or '-h' in args or '--help' in args:
        print_usage()

    savedirs = []
    files = []
    for fname in args[1:]:
        path = os.path.dirname(fname)
        if len(path) == 0:
            path = '.'

        savedirs.append(path)
        filename = os.path.basename(fname)
        if re.match('OMI_BEHR', filename) is None:
            print('{0} does not appear to be a BEHR file (does not start with OMI_BEHR)'.format(filename), file=sys.stderr)
            exit(1)
        files.append(filename)

    return savedirs, files


def split_swaths(filepath, filename):
    f = h5py.File(os.path.join(filepath, filename), 'r')
    ext_ind = filename.rfind('.')
    basename = filename[:ext_ind]
    if filename[ext_ind:] != '.hdf':
        raise RuntimeError('{0} does not end in .hdf. Are you sure it is a BEHR HDFv5 file?'.format(filename))
    swaths = f['Data'].keys()
    for swath in swaths:
        newfile = "{0}-{1}.hdf".format(basename, swath)
        fnew = h5py.File(os.path.join(filepath, newfile))
        g=fnew.create_group('/Data')
        f.copy('/Data/{0}'.format(swath), g, expand_external=True, expand_soft=True, expand_refs=True)
        fnew.close()

    f.close()


if __name__ == "__main__":
    savedirs, origfiles = parse_args(sys.argv)
    for i in range(len(savedirs)):
        split_swaths(savedirs[i], origfiles[i])
