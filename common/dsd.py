import sys
import os
import signal
import difflib
import subprocess
from datetime import datetime
from time import sleep

#####################
# Directory Size Diff (dsd.py) - program to check file system for dir/file size changes by doing before/after comparisons using the "du -k" command every second.
# note: you may have to install the following packages:
#####################

# get the files that will have data to compare
base_name = os.path.join(os.path.dirname(__file__), str('base-df.txt'))
latest_name = os.path.join(os.path.dirname(__file__), str('latest-df.txt'))


def sig_handler(signum, frame):
    """
    handles the <ctrl> + c and cleans up.

    :param signum:
    :param frame:
    :return:
    """
    # is the file exists remove it
    if os.path.isfile(base_name):
        os.remove(base_name)

    # is the file exists remove it
    if os.path.isfile(latest_name):
        os.remove(latest_name)

    print("\nDone")
    sys.exit(0)


# declare signal handler
signal.signal(signal.SIGINT, sig_handler)


def run(path):
    """
    Performs a "du -k <path>" into a file (base_file) and does it again into another file (latest_file) one second later and compares them.

    :param path - The directory to watch
    """
    with open(base_name, "w+") as base_fh:
        # run the "du -k <path>" command for the base-line data
        subprocess.run(["du -k -h -BK", path], stdout=base_fh, stderr=base_fh, shell=True)

        # reset to the start of the file
        base_fh.seek(0)

        # save what was found
        base_text = base_fh.readlines()

        # until <ctrl> + c
        while True:
            # open a file for the latest data
            with open(latest_name, "w+") as latest_fh:
                # run the "du -k <path>" command for the current data
                subprocess.run(["du -k -h -BK", path], stdout=latest_fh, stderr=latest_fh, shell=True)

                # reset to the start of the file
                latest_fh.seek(0)

                # save what was found
                latest_text = latest_fh.readlines()

                # get the diff between the two files
                the_diff = difflib.unified_diff(base_text, latest_text, fromfile='base data', tofile='latest data', lineterm='')

                # init a flag that will be used for user output
                found_diff = False

                # for every diff discovered
                for line in the_diff:
                    # is this the first time in
                    if not found_diff:
                        # what time is it
                        now = datetime.now()

                        # start a new output report
                        print(f'--- Change detected in {path} @ {now} ---')
                        found_diff = True

                    # print the diff
                    print(line.rstrip('\n'))

                # if something was found finish up the report
                if found_diff:
                    # save this as a new base-line so any new changes will be displayed
                    base_text = latest_text
                    print('------------------\n')

            # what a second for the next try
            sleep(1)

            # remove the latest data file
            os.remove(latest_name)


if __name__ == '__main__':
    """
    Main entry point
    
    Args expected --path: Path to the directory to be watched.
    """
    from argparse import ArgumentParser

    parser = ArgumentParser(description=run.__doc__)
    parser.add_argument('--path', default=None, help='Directory to watch', type=str, required=True)

    args = parser.parse_args()

    print(f'DataSizeDiff: Working directory path: {args.path} - Hit <ctrl> + c to exit.')
    run(args.path)
