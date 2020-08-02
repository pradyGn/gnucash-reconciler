import argparse
import importer
import reconciler
import db_setup
import remover

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command')

    # import
    import_parser = subparsers.add_parser('import')
    sources = ["Gnucash", "Institution"]
    import_parser.add_argument("-s", "--source", choices=sources, dest="source", required=True)
    import_parser.add_argument("-a", "--account", dest="account", required=False, help="target account description or ID")
    import_parser.add_argument("-f", "--file", dest="fname", required=True)
    import_parser.add_argument("-i", "--infer", action='store_true', dest="infer", help="infer dates from filename, format: file-name_{{startdate}}_{{enddate}}.csv dates YYYYMMDD")
    import_parser.add_argument("--startdate", dest="start_date", required=False, help="start date formatted as YYYY-MM-DD")
    import_parser.add_argument("--enddate", dest="end_date", required=False, help="end date formatted as YYYY-MM-DD")

    # db setup
    setup_parser = subparsers.add_parser('create-tables')
    setup_parser.add_argument("--drop", action="store_true", dest="drop", help="drop existing tables")
    setup_parser.add_argument("--force", action="store_true", dest="force", help="force dropping tables without prompt")

    # reconcile
    reconcile_parser = subparsers.add_parser('reconcile')
    reconcile_parser.add_argument("--exact-only", action="store_true", dest="exact_only")
    reconcile_parser.add_argument("--reset-transactions", action="store_true", dest="reset_transactions")

    # remove
    remove_parser = subparsers.add_parser('remove')
    remove_parser.add_argument("--batch-id", dest="batchid", required=True)

    args = parser.parse_args()
    if args.command == "create-tables":
        db_setup.create_tables(args.drop, args.force)
    elif args.command == "import":
        importer.import_file(args.fname, args.source, args.account, args.infer, args.start_date, args.end_date)
    elif args.command == "reconcile":
        reconciler.reconcile(args.exact_only, args.reset_transactions)
    elif args.command == "remove":
        remover.remove_batch(args.batchid)


if __name__ == "__main__":
    main()
