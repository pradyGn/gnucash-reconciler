import argparse
import importer
import reconciler
import db_setup

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command')

    # import
    import_parser = subparsers.add_parser('import')
    sources = ["checking", "credit", "gnucash"]
    import_parser.add_argument("--source", choices=sources, dest="source", required=True)
    import_parser.add_argument("--file", dest="fname", required=True)
    import_parser.add_argument("--startdate", dest="start_date", required=True)
    import_parser.add_argument("--enddate", dest="end_date", required=True)

    # db setup
    setup_parser = subparsers.add_parser('create-tables')
    setup_parser.add_argument("--drop", action="store_true", dest="drop")

    # reconcile
    reconcile_parser = subparsers.add_parser('reconcile')
    reconcile_parser.add_argument("--exact-only", action="store_true", dest="exact_only")
    reconcile_parser.add_argument("--reset-transactions", action="store_true", dest="reset_transactions")

    args = parser.parse_args()
    if args.command == "create-tables":
        db_setup.create_tables(args.drop)
    elif args.command == "import":
        importer.import_file(args.fname, args.source, args.start_date, args.end_date)
    elif args.command == "reconcile":
        reconciler.reconcile(args.exact_only, args.reset_transactions)



main()
