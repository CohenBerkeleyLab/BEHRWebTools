from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from __builtin__ import int

import h5py
import os
import re
import sys
import argparse

class VariableError(ValueError):
    def __init__(self, variables):
        if isinstance(variables, list):
            variables = ', '.join(variables)

        super()

class CreateList(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
         if nargs is not None:
             raise ValueError("nargs not allowed")
         super(CreateList, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
         split_vals = values.split(',')
         setattr(namespace, self.dest, split_vals)

def shell_error(msg, errorcode=1):
    print(msg, file=sys.stderr)
    exit(errorcode)

def get_args():
    parser = argparse.ArgumentParser(description='Extract a BEHR variable from HDF5 to a CSV file')
    parser.add_argument('file_in', type=str, help='the .hdf file(s) to extract variables from', nargs='+')
    parser.add_argument('--vars', type=str, action=CreateList, default=['Latitude','Longitude','BEHRColumnAmountNO2Trop'], help='the variable(s) to extract from the listed files')
    parser.add_argument('--out-prefix', type=str, default='', help='the prefix for the output files, default is the same as the input')
    parser.add_argument('--no-merge-vars', action='store_true', help='put output variables in separate files.')
    parser.add_argument('--no-merge-swaths', action='store_true', help='put swaths in separate files.')
    parser.add_argument('--merge-days', action='store_true', help='put all files specified into one output. Conflicts with --no-merge-swaths')

    args = parser.parse_args()
    if args.merge_days and args.no_merge_swaths:
        shell_error('--merge-days and --no-merge-swaths are mutually exclusive')

    return args

def write_header(file_in, file_out, vars):
    if not isinstance(file_in, h5py._hl.files.File):
        raise TypeError('file_in must be an instance of h5py._hl.files.File, typically returned from h5py.File()')
    elif not isinstance(file_out, file)
        raise TypeError('file_out must be an instance of file')
    elif 'w' not in file_out.mode and 'a' not in file_out.mode:
        raise IOError('file_out must be opened for writing (using w or a)')

    swath_name = file_in['Data'].keys()[0]
    swath = file_in['Data'][swath_name]

def main():
    args = get_args()


if __name__ == "__main__":
    main()
