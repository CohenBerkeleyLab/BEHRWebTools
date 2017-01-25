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

        super(VariableError, self).__init__("The following variables could not be found: {0}".format(variables))

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
    #parser.add_argument('--merge-days', action='store_true', help='put all files specified into one output. Conflicts with --no-merge-swaths')

    args = parser.parse_args()
    if args.merge_days and args.no_merge_swaths:
        shell_error('--merge-days and --no-merge-swaths are mutually exclusive')

    return args

def outfile_name_parts(args, file_in_name, swath=None, var=None):
    match = re.search('\d\d\d\d\d\d\d\d', file_in_name)
    datestr = match.group()
    if args.out_prefix == '':
        prefix = file_in_name[:match.start()].rstrip('_')
    else:
        prefix = args.out_prefix.rstrip('_')

    file_out_name = prefix + '_' + datestr
    if swath is not None:
        file_out_name += '_' + swath
    if var is not None:
        file_out_name += '_' + var

    file_out_name += '.csv'
    return file_out_name

def write_header(file_in, file_out, vars):
    if not isinstance(file_in, h5py._hl.files.File):
        raise TypeError('file_in must be an instance of h5py._hl.files.File, typically returned from h5py.File()')
    elif not isinstance(file_out, file):
        raise TypeError('file_out must be an instance of file')
    elif 'w' not in file_out.mode and 'a' not in file_out.mode:
        raise IOError('file_out must be opened for writing (using w or a)')

    swath_name = file_in['Data'].keys()[0]
    swath = file_in['Data'][swath_name]
    header = ['AcrossTrackInd', 'AlongTrackInd']
    for v in vars:
        try:
            shape = swath[v].shape
        except KeyError:
            shell_error('The variable {0} is not present in {1}'.format(v, file_in.filename))

        if len(shape) <= 2:
            header.append(v)
        elif len(shape) == 3:
            for i in range(1,shape[-1]+1):
                header.append('{0}-{1:02}'.format(v, i))
        else:
            raise RuntimeError('{0} dimensional variables not implemented'.format(len(shape)))

    file_out.write(','.join(header)+'\n')

def write_vars(swath, file_out, vars):
    if not isinstance(swath, h5py._hl.group.Group):
        raise TypeError('swath must be an instance of h5py._hl.group.Group')
    elif not isinstance(file_out, file):
        raise TypeError('file_out must be an instance of file')
    elif 'w' not in file_out.mode and 'a' not in file_out.mode:
        raise IOError('file_out must be opened for writing (using w or a)')

    tst = [isinstance(x,str) for x in vars]
    if not isinstance(vars, list) or not all(tst):
        raise TypeError('vars must be a list of strings')

    # Write the along and across track indicies first, then each variable
    shape = swath['Longitude'].shape
    for i in range(shape[0]):
        for j in range(shape[1]):
            line = [str(i),str(j)]
            for v in vars:
                if len(swath[v].shape) == 3:
                    vals = swath[v][i, j, :]
                else:
                    vals = swath[v][i, j]

                for val in vals:
                    line.append(str(val))

            file_out.write(','.join(line) + '\n')

def main():
    args = get_args()
    for f in args.file_in:
        h5f = h5py.File(f)
        for swath in h5f['Data']:
            if args.no_merge_swaths or swath == h5f['Data'].keys()[0]:
                if args.no_merge_swaths:
                write_header(h5f, )


if __name__ == "__main__":
    main()
