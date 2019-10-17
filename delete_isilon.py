#!/usr/bin/env python3

import datetime
import re
import subprocess
import os 
import pandas as pd
import argparse

def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a","--archive",  help="Archive to path instead of delete", type=validate_path)
    parser.add_argument("-p", "--path", help="Enter storage path", type=validate_path)
    parser.add_argument("-d","--day",  help="Enter day policy", type=int)
    parser.add_argument("-e","--exception",  help="Enter path to exception file (Excel format)", type=validate_file)
    parser.add_argument("--dry-run",  help="Show the files to be deleted", action="store_true" )
    return parser.parse_args()

def validate_path(arg, secret=re.compile(r"((?:^/)(?:\w[\.\/-]?)+(?:\w)(?:\/$))")):
    if not secret.match(arg):
        raise argparse.ArgumentTypeError("Invalid value, must start with / and must end with /")
    return arg

def validate_file(arg, file=re.compile(r".*\.(xlsx|xls)")):
    if not file.match(arg):
        raise argparse.ArgumentTypeError("Invalid Excel file")
    return arg

def gdelete_list(ndays, path):
    delete_date = datetime.datetime.today().date() - datetime.timedelta(days=ndays)
    delete_list = []
    try:
        get_files = subprocess.Popen(["hdfs", "dfs", "-ls", "-R", path], stdout=subprocess.PIPE, bufsize=1)
    except subprocess.CalledProcessError:
        print("something bad happened")
        os.exit(1)
    for line in get_files.stdout:
        pattern = re.search("([drwx-]{10})(?:.*)([0-9]{4}-(?:[0-9]{2}-?){2})(?:\s[0-9]{2}:[0-9]{2}\s)(.*)",str(line))
        try:
            if re.match("(^-(?:\w+[-]+\w+?)+)", pattern.group(1), re.IGNORECASE):
                file_date = datetime.datetime.strptime(pattern.group(2), '%Y-%m-%d').date()
                if delete_date >= file_date:
                    delete_list.append(pattern.group(3).strip("'").replace("\\n",""))

        except AttributeError as error:
            continue
    return delete_list

def read_exceptions_excel(file_path):
    try:
        excel = pd.read_excel(file_path)
        column_archive = pd.DataFrame(excel, columns = ['Archivo'])
        exception_list = [item for sublist in column_archive.values.tolist() for item in sublist]
    except FileNotFoundError as error:
        print(error)
        os._exit(1)
    return exception_list

def delete_files(delete_list):
    for file in delete_list:
        output = subprocess.run(["hdfs", "dfs", "-rm", "-f", file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if output.returncode != 0:
            print("Something bad happened while deleting: " + file)
            continue
        else:
            print("Successfully deleted: " + file)

def move_files(delete_list, archive):
    try:
        if subprocess.call(["hdfs", "dfs", "-test", "-e", archive], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 1:
            print("Directory doesn't exist,  creating dir...")
            output = subprocess.run(["hdfs", "dfs", "-mkdir", "-p", archive], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            if output.returncode != 0 and output.returncode is not None:
                raise Exception(output.stdout)
            print("Dir. created")
        else:
            print("Dir. exists")
    except Exception as hdfserror:
        print("There's a problem with path " + archive)
        print(hdfserror)
        os._exit(1)

    for file in delete_list:
        output = subprocess.run(["hdfs", "dfs", "-mv", file, archive], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if output.returncode != 0:
            print("Something bad happened while moving: " + file)
            print(output.stdout)
        else:
            print("Successfully moved: " + file)

if __name__ == '__main__':
    args = read_args()
    if args.path and args.day or args.day is 0: 
        delete_list = gdelete_list(args.day,args.path)
        if args.exception:
            exception = read_exceptions_excel(args.exception)
            new_del_list = [item for item in delete_list if item not in exception]
        else:
            new_del_list = delete_list
    else:
        print("Missing arguments -p and -d")
        os._exit(1)
    if args.dry_run:
        for file in new_del_list:
            print(file)
        print("Files %s deleted" % len(new_del_list))
    else:
						if args.archive:
								move_files(new_del_list, str(args.archive))
						else:
								delete_files(new_del_list)
