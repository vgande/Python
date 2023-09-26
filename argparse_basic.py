import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-r', nargs='+')
parser.add_argument('args_list', nargs='+')
parser.add_argument('-c', nargs='+')
args = parser.parse_args()

print(args)
