import pandas as pd
import params
import numpy as np
import csv
from sqlalchemy import create_engine
import datetime
from loguru import logger
import remover

"""     DATABASE    """
engine = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}" \
                       .format(user=params.db_user, password=params.db_pass, host=params.db_host, port=params.db_port,
                               db=params.db_name))

"""     DATATYPES   """

def convert_amounts(df):
    df['Amount'] = df['Amount'].astype("float64")
    return df


def convert_dates(df, columns):
    for col in columns:
        df[col] = df[col].astype("datetime64")
    return df


"""    SHARED    """

def create_accounts_map():
    """Create a map from the Gnucash Account Name to Account ID"""
    results = pd.read_sql("select `GnucashName`, `ID` from `Accounts`;", engine)
    accounts_map = {}
    for idx, entry in results.iterrows():
        accounts_map[entry['GnucashName']] = entry['ID']
    return accounts_map


"""     GNUCASH     """


def parse_gnucash_descr(raw):
    split = raw.strip(')').split(" (")
    if len(split) == 1:
        return split[0], split[0]
    elif len(split) == 2:
        return split[0], split[1]
    else:
        raise Exception("couldn't parse %s" % raw)


def get_merchant(raw):
    return parse_gnucash_descr(raw)[1]


def get_descr(raw):
    return parse_gnucash_descr(raw)[0]


def gnucash_mapper(fname, account_id, batch_id, start_date, end_date):
    accounts_map = create_accounts_map()

    df = pd.read_csv(fname).fillna(method='ffill')
    original_length = len(df)
    df = df[df['Account Name'].isin(accounts_map.keys())]
    if len(df) < original_length:
        logger.warning(f"Dropped {original_length - len(df)} rows with unknown account names")
    df = df.filter(items=['Date', 'Description', 'Amount Num.', 'Account Name']) \
        .rename(columns={'Amount Num.': 'Amount', 'Account Name': 'Account'})
    df = convert_dates(df, ('Date',))

    # validate date
    df['BatchID'] = batch_id
    for idx, entry in df.iterrows():
        if (entry['Date'].date() < start_date) or (entry['Date'].date() > end_date):
            print("Found entry with date outside specified range, aborting: {}".format(entry))

    df['Merchant'] = df['Description'].astype(str).apply(get_merchant)
    df['Description'] = df['Description'].astype(str).apply(get_descr)
    df['AccountID'] = df['Account'].apply(lambda x: accounts_map[x])
    df = df.drop(columns=['Account'])
    df['Amount'] = df['Amount'].astype(str).apply(lambda x: x.replace(",", ""))
    df = convert_amounts(df)
    
    table = 'Gnucash'
    df.to_sql(name=table, con=engine, if_exists='append', index=False)


"""     CREDIT      """


def chase_credit_mapper(fname, account_id, batch_id, start_date, end_date):
    df = pd.read_csv(fname)
    df = df.filter(items=['Transaction Date', 'Post Date', 'Description', 'Amount']) \
        .rename(columns={'Transaction Date': 'TransactionDate', 'Post Date': 'PostingDate'})
    df = convert_dates(df, ('PostingDate', 'TransactionDate'))

    # validate date

    df['BatchID'] = batch_id
    for idx, entry in df.iterrows():
        if (entry['PostingDate'].date() < start_date) or (entry['PostingDate'].date() > end_date):
            print("Found entry with posting date outside specified range, aborting: {}".format(entry))

    df['AccountID'] = account_id
    df = convert_amounts(df)
    
    df['Date'] = df.apply(lambda row : get_best_date(row), axis=1)
    
    table = "Institution"
    df.to_sql(name=table, con=engine, if_exists='append', index=False)


"""     CHECKING    """


def parse_chase_date(entry):
    descr = entry['Description']
    postDate = entry['PostingDate']
    dash_ind = descr.rfind('/')
    # get year
    descrMonth = descr[dash_ind - 2:dash_ind]
    postingMonth = postDate.split('/')[0]
    if descrMonth == "12" and postingMonth == "01":
        year = str(int(postDate.split('/')[-1]) - 1)
    else:
        year = postDate.split('/')[-1]
    # if we can't find a date simply use posting date
    if dash_ind == -1:
        return None
    else:
        datestring = descr[dash_ind - 2:dash_ind + 3] + '/' + year
        return pd.to_datetime(datestring)


def get_best_date(entry):
    if pd.isnull(entry['TransactionDate']):
        return entry['PostingDate']
    else:
        return entry['TransactionDate']


def chase_checking_mapper(fname, account_id, batch_id, start_date, end_date):
    with open(fname) as f:  # pandas misreading chase csv
        df = [{k: v for k, v in row.items()}
              for row in csv.DictReader(f, skipinitialspace=True)]
    df = pd.DataFrame(df)
    df = df.filter(items=['Posting Date', 'Description', 'Amount']) \
        .rename(columns={'Posting Date': 'PostingDate'})
    df['TransactionDate'] = df.apply(parse_chase_date, axis=1)
    df = convert_dates(df, ('PostingDate', 'TransactionDate'))
    df['Date'] = df.apply(lambda row : get_best_date(row), axis=1)

    # validate date
    df['BatchID'] = batch_id
    for idx, entry in df.iterrows():
        if (entry['PostingDate'].date() < start_date) or (entry['PostingDate'].date() > end_date):
            print("Found entry with posting date outside specified range, aborting: {}".format(entry))

    df['AccountID'] = account_id

    df = convert_amounts(df)

    table = "Institution"
    df.to_sql(name=table, con=engine, if_exists='append', index=False)


def dates_are_valid(start_date, end_date, source, account_id):
    account_id_matcher = "= " +str(account_id) if account_id is not None else "IS NULL"

    # check start before or equal to end
    if end_date < start_date:
        logger.info("End date cannot be before start date")
        return False

    # validate batch to ensure no overlap
    # case 1 : supplied end date >= start date and supplied start date <= end date - BAD
    pre_overlap_query = f"SELECT * FROM Batch WHERE `Source` = '{source}' AND `AccountID` {account_id_matcher} AND `StartDate` <= '{end_date}' AND `EndDate` >= '{start_date}'"
    pre_results = pd.read_sql_query(pre_overlap_query, engine)
    if len(pre_results) != 0:
        print("Found overlapping batch in case 1, aborting: \n{}".format(str(pre_results)))
        return False

    # case 2 : supplied end date <= start date and supplied end date >= start date - BAD
    mid_overlap_query = f"SELECT * FROM Batch WHERE `Source` = '{source}' AND `AccountID` {account_id_matcher} AND `StartDate` >= '{start_date}' AND `StartDate` <= '{end_date}'"
    mid_results = pd.read_sql_query(mid_overlap_query, engine)
    if len(mid_results) != 0:
        print("Found overlapping batch in case 2, aborting: \n{}".format(str(mid_results)))
        return False
    
    return True

def get_account_id(name_or_id):
    if name_or_id.isnumeric():
        results = pd.read_sql(f"SELECT `ID` FROM `Accounts` WHERE `ID` = {name_or_id}", engine)
    else:
        results = pd.read_sql(f"SELECT `ID` FROM `Accounts` WHERE `Description` = '{name_or_id}'", engine)
    if len(results) != 1:
        raise ValueError(f"Found {len(results)} matches for account {name_or_id}")

    return results.iloc[0]['ID']

mapper_dict = {'Gnucash': gnucash_mapper,
               'ChaseChecking': chase_checking_mapper,
               'ChaseCredit': chase_credit_mapper}


def import_file(fpath, source, account_name_or_id, infer, start_date, end_date):

    if source == "Institution":
        account_id = get_account_id(account_name_or_id)
    else:
        # no account id for gnucash
        account_id = None

    if infer:
        # grab filename
        fname = fpath.split('/')[-1]
        # remove extension
        fname = fname[:fname.rfind(".")]
        # parse dates
        splitted = fname.split('_')
        start_date = datetime.datetime.strptime(splitted[-2], "%Y%m%d").date()
        end_date = datetime.datetime.strptime(splitted[-1], "%Y%m%d").date()
        

    elif start_date is None or end_date is None:
        raise ValueError("Infer was false but start_date and end_date not specified")

    else:
        # convert date strings
        start_date = list(map(lambda x : int(x), start_date.split('-')))
        start_date = datetime.date(start_date[0], start_date[1], start_date[2])
        end_date = list(map(lambda x : int(x), end_date.split('-')))
        end_date = datetime.date(end_date[0], end_date[1], end_date[2])

    # ensure no overlaps
    if not dates_are_valid(start_date, end_date, source, account_id):
        return

    # create new batch
    new_batch = pd.DataFrame(data={'StartDate': [start_date], 'EndDate': [end_date], 'Source': [source], 'AccountID': [account_id]}).to_sql(con=engine, name='Batch', if_exists='append', index=False)
    account_id_matcher = "= " + str(account_id) if account_id is not None else "IS NULL"
    mysql_fmt = "%Y-%m-%d"
    get_id_query = f"SELECT ID FROM `Batch` WHERE `StartDate` = '{start_date.strftime(mysql_fmt)}' AND `AccountID` {account_id_matcher} AND `EndDate` = '{end_date.strftime(mysql_fmt)}' AND `Source` = '{source}'"
    batch_id = pd.read_sql(get_id_query, engine).iloc[0]['ID']

    # TODO: must drop batch on failure

    # run import
    try:
        if source == "Gnucash":
            mapper_name = source
        else:
            mapper_name = pd.read_sql(f"SELECT `Mapper` from Accounts WHERE ID = {account_id}", engine).iloc[0]['Mapper']
        mapper = mapper_dict[mapper_name]
        mapper(fpath, account_id, batch_id, start_date, end_date)

    except Exception:
        remover.remove_batch(batch_id, quiet=True)