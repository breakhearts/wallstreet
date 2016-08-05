import argparse
import sys
from wallstreet.tasks.stock_history_tasks import update_all_stock_info, update_all_stock_day

parser = argparse.ArgumentParser(description="wallstreet console")
parser.add_argument('--info', dest="update", action='store_true', const=update_all_stock_day, default=update_all_stock_info )
args = parser.parse_args(sys.argv[1:])


def main():
    args.update()


if __name__ == "__main__":
    main()
