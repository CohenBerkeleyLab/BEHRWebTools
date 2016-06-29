#!/bin/bash
# Usage: get_BEHR.sh data_type [start_date] [end_date]
#
# Invoke this help with the "-h" or "--help" arguments.
# Additional warranty information: invoke with "--info"
#
# This script will download BEHR data from http://behr.cchem.berkeley.edu/DownloadBEHRData.aspx
# in batch. The first argument, can be one of: "txt", "hdf", or "gridded", and specifies which
# type of data to retrieve. "txt" will retrieve the native OMI pixel size data in text format.
# "hdf" will retrieve the native OMI pixel size data in HDF (version 5) format. "gridded" will
# retrieve the BEHR data gridded to 0.05x0.05 degrees in HDF (version 5) format.
#
# The second and third arguments can be used to specify a date range. If only the beginning is 
# given, it will assume that you want all the BEHR data from that date to the present. If neither
# date is specified, then it will assume that you want to update your local files. Thus it will
# download all BEHR files forward from the chronologically last one you have locally, or if you
# have no files locally, the entire record for the datatype specified.
#
# Finally, this requires that the directories in which to save the requested data types be
# specified.  This is done by one of two mechanisms.  First, the paths can be directly entered
# into the get_BEHR.sh file as the variables txtdir, hdfdir, or griddir. (It should be fairly
# obvious where to enter them.) Alternatively, the paths can be specified as environmental 
# variables in your .profile or .bashrc files. They must be specified as the variables 
# BEHR_TXTDIR, BEHR_HDFDIR, and BEHR_GRIDDIR.  If you are uncertain what that means, you should
# simply modify them in the get_BEHR.sh script. Those defined as environmental variables
# take precedence over those in the script.
#
# Three additional options can be specified:
#   -n, --no-ask: will turn off the interactive prompt asking you to confirm the download settings.
#   -q, --quiet: will turn off all of the output to stdout.
#   --wget-progress: turns on normal wget output
#
# As a final note, the exit error codes are based off of those in /usr/include/sysexits.h, if you
# wish to see the meaning of the numeric exit code for an error.

# The save directory can be specified here or in the .bashrc file on Linux systems
# or .profile file on Macs. If given in the .bashrc or .profile files as environmental
# variables, those will supercede the values set here.

##### USER MAY MODIFY THESE PATHS #####
txtdir="/Users/Josh/Downloads/TXT"
hdfdir="/Users/Josh/Downloads/HDF"
griddir="/Users/Josh/Downloads/GRID"
##### END USER SET PATHS #####

DEBUG=false

me=$0
myname=$(basename $0)

print_help() {
    while read p
    do
        if [[ ${p:0:2} == "#!" ]]; then
            continue
        elif [[ ${p:0:1} == "#" ]]; then
            echo "${p:1}"
        else
            break
        fi
    done<$me
    exit 0
}

print_warranty() {
    echo ""
    echo " *******************************************************************"
    echo " This is free software, you may modify and redistribute it provided:"
    echo "  1) Attribution is given to the original author"
    echo "  2) This warranty is retained in full in the modified product"
    echo ""
    echo " The author of this program provides ABSOLUTELY NO WARRANTY to the"
    echo " extent permitted by law."
    echo ""
    echo " Use of this program is AT YOUR OWN RISK. We have tested it"
    echo " extensively, but cannot guarentee its behavior on all computers"
    echo ""
    echo " Bug reports can be made to the current maintainer of the BEHR"
    echo " product (see http://behr.cchem.berkeley.edu/DownloadBEHRData.aspx)"
    echo " Please provide your operating system, the output of bash --version"
    echo " and a detailed description of the problem."
    echo ""
    echo "     Josh Laughner, 10 June 2016"
    echo " *******************************************************************"
    echo ""
    
    exit 0
}

test_date() {
    datein="$1"
    [[ $datein =~ ^[0-9]{4}(0[0-9]|1[0-2])([0-2][0-9]|3[0-1])$ ]] && return 0 || return 1
}

check_date() {
    datein="$1"
    datename="$2"

    if $DEBUG; then echo "Checking $datename $datein"; fi

    if [[ ! -z "$datein" ]]
    then
        test_date "$datein"
        stbool=$?
        if [[ $stbool != 0 ]]
        then
            echo "error in $myname: The $datename is formatted incorrectly: it must be YYYYMMDD, e.g. 20160531 for May 31, 2016."
            exit 64
        fi
    fi
}

# Check that we are not running as root
if [[ $EUID == 0 ]]
then
    echo "error in $myname: Please do not run this script as root either directly or using SUDO."
    exit 1
fi

# Parse all arguments to check if "-h" or "--help" is one of them
if [[ $# == 0 ]]
then
    print_help
fi

askbool=true
verbbool=true
wgerverb=false

while (( "$#" ))
do
    case "$1" in
    -h|--help)
        print_help
        ;;
    --info)
        print_warranty
        ;;
    -q|--quiet)
        verbbool=false
        ;;
    -n|--no-ask)
        askbool=false
        ;;
    --wget-progress)
        wgetverb=true
        ;;
    *)
        # Handle the positional parameters
        if [[ -z $datatypein ]]; then
            datatypein="$1"
        elif [[ -z $startdatein ]]; then
            startdatein="$1"
        elif [[ -z $enddatein ]]; then
            enddatein="$1"
        else
            echo "error in $myname: unrecognized parameter $1"
        fi
        ;;
    esac
    
    shift
done

if $DEBUG; then
    echo "verbbool=$verbbool"
    echo "askbool=$askbool"
    echo "datatypein=$datatypein"
    echo "startdatein=$startdatein"
    echo "enddatein=$enddatein"
    echo ""
fi

# Otherwise check that the arguments are what is expected. The first should be one of
# "txt", "hdf", or "gridded" and the second and third should either be empty or dates
# in the yyyymmdd format. Exit code 64 is defined in /usr/include/sysexits.h as the
# error for improper usage.  Considering the lack of standardized errors for scripting
# (that file is technically meant for C programmers) we'll use it here.
allowed_formats="txt, hdf, gridded"
if [[ -z $datatypein ]]
then
    echo "error in $myname: requires at least 1 argument to specify the type of data to retrieve ($allowed_formats)"
    exit 64
elif [[ $allowed_formats != *"$datatypein"* ]]
then
    echo "error in $myname: first arguement to get_BEHR.sh must be one of $allowed_formats"
    exit 64
else
    datatype=$datatypein
fi

# Get the paths. Look for the environmental variables first, then the directory variables
# in here. In either case, make sure the directory is valid (and it is a directory).
if [[ $datatype == "txt" ]]
then
    remotedir='http://behr.cchem.berkeley.edu/behr/behr_txt'
    fileext="txt"
    if [[ -z $BEHR_TXTDIR ]]
    then
        datadir=$txtdir
        direrrstr="the variable txtdir in $myname"
    else
        datadir=$BEHR_TXTDIR
        direrrstr="the environmental variable BEHR_TXTDIR"
    fi
elif [[ $datatype == "hdf" ]]
then
    remotedir='http://behr.cchem.berkeley.edu/behr/behr_hdf'
    fileext="hdf"
    if [[ -z $BEHR_HDFDIR ]]
    then
        datadir=$hdfdir
        direrrstr="the variable hdfdir in $myname"
    else
        datadir=$BEHR_HDFDIR
        direrrstr="the environmental variable BEHR_HDFDIR"
    fi
elif [[ $datatype == "gridded" ]]
then
    remotedir='http://behr.cchem.berkeley.edu/behr/behr_regridded_hdf'
    fileext="hdf"
    if [[ -z $BEHR_GRIDDIR ]]
    then
        datadir=$griddir
        direrrstr="the variable griddir in $myname"
    else
        datadir=$BEHR_GRIDDIR
        direrrstr="the environmental variable BEHR_GRIDDIR"
    fi
else
    echo "error in $myname: no case implemented for the data type $datatype"
    exit 70
fi

if [[ -z $datadir ]]
then
    echo "error in $myname: the data directory for the data type $datatype does not appear to be defined as either an environmental variable or in $myname."
    exit 73
elif [[ ! -d $datadir ]]
then
    echo "error in $myname: the directory $datadir does not exist. Either create it, or correct the path defined in $direrrstr."
    exit 73
fi

# Of course we need to know which version of date we are using!
# Credit to http://stackoverflow.com/questions/8747845/how-can-i-detect-bsd-vs-gnu-version-of-date-in-shell-script

if date --version >/dev/null 2>&1
then
    gnudate=true
else
    gnudate=false
fi

# Verify the start and end dates, or set them as necessary
if [[ -z "$startdatein" ]]
then
    if $DEBUG; then echo "Finding most recent file in $datadir"; fi
    # find the most recent BEHR file of the specified type
    lastfile=$(ls -1 $datadir/ | tail -n 1)
    if $DEBUG; then echo "Most recent file is $lastfile"; fi
    
    regex="[0-9]{8}"
    if [[ $lastfile =~ $regex ]]
    then 
        startdate=${BASH_REMATCH[0]}
        if $DEBUG; then echo "Setting startdate ($startdate) based on last file"; fi
        
        if $gnudate
        then    
            startdate=$(date -d "$startdate + 1 day" +'%Y%m%d')
        else
            startdate=$(date -v +1d -j -f '%Y%m%d' "$startdate" +'%Y%m%d')
        fi
    else
        if $DEBUG; then echo "Setting startdate to beginning of BEHR record"; fi
        startdate="20050101" # earlier BEHR file
    fi
    if $DEBUG; then echo ""; fi
else
    check_date "$startdatein" "starting date"
    startdate="$startdatein"
fi

if [[ -z "$enddatein" ]]
then
    # make the last day allowed into today
    enddate=$(date +'%Y%m%d')
else
    check_date "$enddatein" "ending date"
    enddate="$enddatein"
fi



# If running without the -n flag, this will check with the user that it is downloading data
# to the proper place.
if $verbbool || $askbool
then
    echo "$myname: will retrieve $datatype for the period $startdate to $enddate"
    echo "         and store them in $datadir."
fi

if $askbool
then
    read -r -p "         Is this correct? [y/N]: " response
    case $response in
        [yY][eE][sS]|[yY])
            ;;
        *)
            echo "         Cancelling download."
            exit 0
            ;;
    esac
fi

if $verbbool
then
    echo "         Now downloading BEHR files..."
fi

# Now we need to actually get the data. This will be a little different than
# my MODIS download script, because since I know the file patterns, I can
# give wget a pattern to find for each day.

# We also need the current version string
behrver=$(curl --silent http://behr.cchem.berkeley.edu/behr/behr_version.txt)

currdate=$startdate
wgetopts="--no-host-directories --no-directories --directory-prefix=$datadir"
if [[ $wgetverb != true ]]
then
    wgetopts="$wgetopts --quiet"
fi

loopcount=1

while [[ $currdate -le $enddate ]]
do

    if $DEBUG; then echo "Loop $loopcount, current date = $currdate"; fi

    filename="OMI_BEHR_${behrver}_$currdate.$fileext"
    # Try downloading the file pattern
    if $verbbool; then echo "Now downloading $remotedir/$filename..."; fi
    wget $wgetopts $remotedir/$filename
    if [[ $? != 0 && $verbbool ]]; then echo "   $filename does not exist."; fi

    # Advance the date by one. GNU date seems to accept yyyymmdd as an
    # input format.
    if $gnudate
    then    
        currdate=$(date -d "$currdate + 1 day" +'%Y%m%d')
    else
        currdate=$(date -v +1d -j -f '%Y%m%d' "$currdate" +'%Y%m%d')
    fi
        
    loopcount=$((loopcount+1))
done
