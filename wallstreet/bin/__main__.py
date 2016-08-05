import argparse
import sys
from wallstreet.tasks.stock_history_tasks import update_all_stock_info, update_all_stock_day

parser = argparse.ArgumentParser(description="wallstreet console")
parser.add_argument('--info', dest="info", action='store_true')
parser.add_argument('--day', dest="day", action='store_true')
args = parser.parse_args(sys.argv[1:])


def main():
    if args.info:
        update_all_stock_info()
    elif args.day:
        update_all_stock_day()

if __name__ == "__main__":
    main()
