import argparse
import sys
from wallstreet.tasks.stock_tasks import update_all_stock_info, update_all_stock_day, update_all_stock_base_index
from wallstreet.tasks.stock_tasks import update_all_year_fiscal_report, update_all_stock_info_details
from wallstreet.tasks.stock_tasks import update_all_sec_fillings_idx, update_sec_fillings, update_all_sec_fillings

parser = argparse.ArgumentParser(description="wallstreet console")
parser.add_argument('--info', dest="info", action='store_true', help="update stock info")
parser.add_argument('--day', dest="day", action='store_true', help="update stock day")
parser.add_argument('--index', dest="index", action='store_true', help="update stock base index")
parser.add_argument('--year', dest="year", action='store_true', help="update stock year fiscal report(Edgar API)")
parser.add_argument('--company', dest="company", action='store_true', help="update company info")
parser.add_argument('--idx', dest="idx",  nargs=4,   metavar=("start_year", "start_quarter", "end_year", "end_quarter"),
                    help="update sec fillings index")
parser.add_argument('--filling', dest="filling",  nargs=5,
                    metavar=("symbol", "filling_type(txt/xbrl)", "form_type(eg, 10-K:10K/A:10-K405:10-K405/A)", "start_date", "end_date"),
                    help="update sec fillings by symbol")
parser.add_argument('--filling_all(', dest="filling_all",  nargs=4,
                    metavar=("filling_type(txt/xbrl)", "form_type(eg, 10-K:10K/A:10-K405:10-K405/A)", "start_date", "end_date"),
                    help="update all sec fillings")
args = parser.parse_args(sys.argv[1:])


def main():
    if args.info:
        update_all_stock_info()
    elif args.day:
        update_all_stock_day()
    elif args.index:
        update_all_stock_base_index()
    elif args.year:
        update_all_year_fiscal_report()
    elif args.company:
        update_all_stock_info_details()
    elif args.idx and len(args.idx) > 0:
        start_year, start_quarter, end_year, end_quarter = args.idx
        update_all_sec_fillings_idx(int(start_year), int(start_quarter), int(end_year), int(end_quarter))
    elif args.filling and len(args.filling) > 0:
        symbol, filling_type, form_type, start_date, end_date = args.filling
        assert filling_type in ("txt", "xbrl")
        form_type = form_type.split(":")
        update_sec_fillings(symbol, filling_type, form_type, start_date, end_date)
    elif args.filling_all and len(args.filling_all) > 0:
        filling_type, form_type, start_date, end_date = args.filling_all
        assert filling_type in ("txt", "xbrl")
        form_type = form_type.split(":")
        update_all_sec_fillings(filling_type, form_type, start_date, end_date)

if __name__ == "__main__":
    main()
