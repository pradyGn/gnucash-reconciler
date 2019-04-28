import pandas as pd
import params
import numpy as np
import csv
from sqlalchemy import create_engine
import datetime
from loguru import logger

"""     DATABASE    """
engine = create_engine("mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}" \
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


def import_gnucash(fname, batch_id, start_date, end_date):
    df = pd.read_csv(fname).fillna(method='ffill')
    df = df[df['Account Name'].isin(params.accounts_map.keys())]
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
    df['Account'] = df['Account'].apply(lambda x: params.accounts_map[x])
    df['Amount'] = df['Amount'].apply(lambda x: x.replace(",", ""))
    df = convert_amounts(df)
    table = 'Gnucash'

    df.to_sql(name=table, con=engine, if_exists='append', index=False)


"""     CREDIT      """


def import_chase_credit(fname, batch_id, start_date, end_date):
    df = pd.read_csv(fname)
    df = df.filter(items=['Transaction Date', 'Post Date', 'Description', 'Amount']) \
        .rename(columns={'Transaction Date': 'TransactionDate', 'Post Date': 'PostingDate'})
    df = convert_dates(df, ('PostingDate', 'TransactionDate'))

    # validate date

    df['BatchID'] = batch_id
    for idx, entry in df.iterrows():
        if (entry['PostingDate'].date() < start_date) or (entry['PostingDate'].date() > end_date):
            print("Found entry with posting date outside specified range, aborting: {}".format(entry))

    df['Account'] = "FreedomCredit"
    table = "Institution"
    df = convert_amounts(df)
    
    df['Date'] = df.apply(lambda row : get_best_date(row), axis=1)
    
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


def import_chase_checking(fname, batch_id, start_date, end_date):
    with open(fname) as f:  # pandas misreading chase csv
        df = [{k: v for k, v in row.items()}
              for row in csv.DictReader(f, skipinitialspace=True)]
    df = pd.DataFrame(df)
    df = df.filter(items=['Posting Date', 'Description', 'Amount']) \
        .rename(columns={'Posting Date': 'PostingDate'})
    df['TransactionDate'] = df.apply(lambda row : parse_chase_date(row), axis=1)
    df = convert_dates(df, ('PostingDate', 'TransactionDate'))
    df['Date'] = df.apply(lambda row : get_best_date(row), axis=1)

    # validate date
    df['BatchID'] = batch_id
    for idx, entry in df.iterrows():
        if (entry['PostingDate'].date() < start_date) or (entry['PostingDate'].date() > end_date):
            print("Found entry with posting date outside specified range, aborting: {}".format(entry))

    df['Account'] = "Checking"

    table = "Institution"
    df = convert_amounts(df)
    df.to_sql(name=table, con=engine, if_exists='append', index=False)


def dates_are_valid(start_date, end_date, source):
    # check start before or equal to end
    if end_date < start_date:
        logger.info("End date cannot be before start date")
        return False

    # validate batch to ensure no overlap
    # case 1 : supplied end date >= start date and supplied start date <= end date - BAD
    pre_overlap_query = "SELECT * FROM Batch WHERE `Source` = '{source}' AND `StartDate` <= '{end_date}' AND 'EndDate' >= '{start_date}'".format(source=source, start_date=start_date, end_date=end_date)
    pre_results = pd.read_sql_query(pre_overlap_query, engine)
    if len(pre_results) != 0:
        print("Found overlapping batch, aborting: \n{}".format(str(pre_results)))
        return False

    # case 2 : supplied end date <= start date and supplied end date >= start date - BAD
    mid_overlap_query = "SELECT * FROM Batch WHERE `Source` = '{source}' AND `StartDate` >= '{start_date}' AND 'StartDate' <= '{end_date}'".format(source=source, start_date=start_date, end_date=end_date)
    mid_results = pd.read_sql_query(mid_overlap_query, engine)
    if len(mid_results) != 0:
        print.info("Found overlapping batch, aborting: \n{}".format(str(mid_results)))
        return False
    
    return True

def import_file(fname, source, start_date, end_date):
    # convert date strings
    start_date = list(map(lambda x : int(x), start_date.split('-')))
    start_date = datetime.date(start_date[0], start_date[1], start_date[2])
    end_date = list(map(lambda x : int(x), end_date.split('-')))
    end_date = datetime.date(end_date[0], end_date[1], end_date[2])

    #ensure no overlaps
    if not dates_are_valid(start_date, end_date, source):
        return

    # create new batch
    new_batch = pd.DataFrame(data={'StartDate': [start_date], 'EndDate': [end_date], 'Source': [source]}).to_sql(con=engine, name='Batch', if_exists='append', index=False)
    get_id_query = "SELECT ID FROM Batch WHERE `StartDate` = '{start_date}' AND `EndDate` = '{end_date}'".format(start_date=start_date, end_date=end_date)
    batch_id = pd.read_sql(get_id_query, engine).iloc[0]['ID']
    # TODO: must drop batch on failure

    # run import
    if source == "gnucash":
        import_gnucash(fname, batch_id, start_date, end_date)
    elif source == "checking":
        import_chase_checking(fname, batch_id, start_date, end_date)
    elif source == "credit":
        import_chase_credit(fname, batch_id, start_date, end_date)


