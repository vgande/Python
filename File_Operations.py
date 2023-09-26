import os
import datetime


def reader(filenm):
    print("Input File Name is {}".format(filenm))
    with open(filenm, "r") as file:
        data = file.readlines()
        for line in data:
            print(line.split("::"))

    file.close()


def listdir(inputdir, excludedir, numofdays=7):

    for root, dirs, files in os.walk(inputdir):
        dirs[:] = [d for d in dirs if d not in excludedir]
        for f in files:
            currfile = os.path.join(root, f)
            file_modified_ts = datetime.datetime.fromtimestamp(os.path.getmtime(currfile))
            if datetime.datetime.now() - file_modified_ts > datetime.timedelta(days=numofdays):
                print(currfile)


def __main__():
    filename = "C:/Users/venki/Downloads/Data/movies.dat"
    inpdir = "C:/Users/venki/Downloads/Data/"
    excludedir = ["Week12", "SQLite", "Phone_Search.parquet"]
    reader(filename)
    listdir(inpdir, excludedir)


__main__()
