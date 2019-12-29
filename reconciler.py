import params
from sqlalchemy import create_engine
import pandas as pd
from datetime import timedelta
from loguru import logger
import sys

date_thresh = timedelta(days=params.date_threshold)
engine = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}" \
                       .format(user=params.db_user, password=params.db_pass, host=params.db_host, port=params.db_port,
                               db=params.db_name))

float_err = 0.00001

def source_to_target_table(source_table):
    if source_table == "Gnucash":
        return "Institution"
    elif source_table == "Institution":
        return "Gnucash"
    else:
        raise ValueError("Invalid table name {}".format(source_table))


def get_unmatched_gnucash():
    query = "SELECT * FROM `Gnucash` WHERE `TransactionID` IS NULL"
    return pd.read_sql(query, engine)


def get_unmatched_institution():
    query = "SELECT * FROM `Institution` WHERE `TransactionID` IS NULL"
    return pd.read_sql(query, engine)


def find_identical(entry, target_table):
    query = """SELECT * FROM `{table}` WHERE `Date` BETWEEN '{date_beg}' AND '{date_end}' AND `Amount` = '{amount}' AND `Account` = '{account}' AND `TransactionID` IS NULL""" \
        .format(table=target_table, account=entry['Account'], amount=entry['Amount'],
                date_beg=entry['Date'] - date_thresh,
                date_end=entry['Date'] + date_thresh)
    return pd.read_sql(query, engine)


def update_transaction_id(table, table_id, transaction_id):
    query = """UPDATE `{table}`
    SET
        `TransactionID` = {transaction_id}
    WHERE
        `ID` = {table_id};""".format(table=table, table_id=table_id, transaction_id=transaction_id)
    with engine.connect() as conn:
        conn.execute(query)


def next_transaction_id():
    # todo: fix this
    create_query = "INSERT INTO TransactionIDSequence VALUES ()"
    get_query = "SELECT MAX(TransactionID) FROM TransactionIDSequence"
    with engine.connect() as conn:
        conn.execute(create_query)
        return conn.execute(get_query).fetchone()["MAX(TransactionID)"]


def multimatch_input_is_valid(val, matches):
    if val == "skip":
        return True
    valid_ids = matches['ID'].tolist()
    try:
        intval = int(val)
        return intval in valid_ids
    except ValueError:
        return False


def process_single_match(gc_entry, inst_entry):
    transaction_id = next_transaction_id()
    logger.debug(
        "Matched Gnucash {gnucash_id} ({descr} on {date}) to Institution {inst_id}, Transaction ID {trans_id}" \
            .format(gnucash_id=gc_entry['ID'], descr=gc_entry['Description'], date=gc_entry['Date'],
                    inst_id=inst_entry['ID'],
                    trans_id=transaction_id))
    update_transaction_id("Gnucash", gc_entry['ID'], transaction_id)
    update_transaction_id("Institution", inst_entry['ID'], transaction_id)


def process_multiple_matches(source, matches, source_table):
    print("\n\nI found {count} matches. Source: ".format(count=len(matches)))
    print(source)
    print("Matches:")
    print(matches)
    target_id = input("Enter the ID of the correct transaction to attach, or 'skip' to continue: ")
    while not multimatch_input_is_valid(target_id, matches):
        target_id = input(
            "Invalid Input. Enter the ID of the correct transaction to attach, or 'skip' to continue: ")
    if target_id != "skip":
        transaction_id = next_transaction_id()
        target_id = int(target_id)
        target_table = source_to_target_table(source_table)
        update_transaction_id(source_table, source['ID'], transaction_id)
        update_transaction_id(target_table, target_id, transaction_id)

    print("\n")


def get_combinations(amt_remaining, unmatched, cur_set, valid_sets):
    logger.debug("unmatched is size {}".format(len(unmatched)))
    cur_set = cur_set.copy()
    logger.debug("new method call, amount remaining is {}, cur_set is {}, have {} valid sets".format(amt_remaining, [entry['Amount'] for entry in cur_set], len(valid_sets)))
    for idx, entry in unmatched.iterrows():
        amt = entry['Amount']
        logger.debug("checking for amount {}".format(entry['Amount']))
        if abs(amt - amt_remaining) < float_err:
            logger.debug("amount matches amount remaining, adding current set to valid sets")
            cur_set.append(entry)
            valid_sets.append(cur_set)
            break
        elif abs(amt) < abs(amt_remaining):
            logger.debug("amount is less than amount remaining, moving to current set and continuing")
            cur_set.append(entry)
            try:
                unmatched.drop(idx, inplace=True)
            except KeyError:
                logger.debug("tried to delete unmatched index {} but was already deleted".format(idx))
            valid_sets.extend(get_combinations(amt_remaining - amt, unmatched, cur_set, []))
        else:
            try:
                unmatched.drop(idx, inplace=True)
            except KeyError:
                logger.debug("tried to delete unmatched index {} but was already deleted".format(idx))
    return valid_sets


def find_sums(entry, table):
    query = "SELECT * FROM `{table}` WHERE `Date` BETWEEN '{date_beg}' AND '{date_end}' AND TransactionID IS NULL AND \
    `Account` = '{account}'".format(table=table, date_beg=entry['Date'] - date_thresh,
                                   date_end=entry['Date'] + date_thresh,
                                   account=entry['Account'])
    if (entry['Amount']) > 0:
        query = query + " AND `Amount` > 0"
    else:
        query = query + "AND `Amount` <= 0"
    unmatched = pd.read_sql(query, engine)
    return get_combinations(entry['Amount'], unmatched, [], [])


def link_multiple(table, summing_entries, transaction_id):
    for entry in summing_entries:
        update_transaction_id(table, entry['ID'], transaction_id)


def multiple_sums_input_is_valid(val, max_option):
    if val == "skip":
        return True
    try:
        val = int(val)
        return val > 0 and val <= max_option
    except ValueError:
        return False


def process_no_matches(source, source_table, ignore_missing=False):

    target_table = source_to_target_table(source_table)
    potentials = find_sums(source, target_table)
    if len(potentials) == 1:
        print("\n\n\nI didn't find a single entry to match, but did find one set of entries that sum to the " +
              "desired amount. Source transaction:")
        print(source)
        print("Summing transactions:")
        print(potentials[0])
        action = input("Type 'link' to link these entries into a transaction or 'skip' to continue without linking: ")
        while action != "skip" and action != "link":
            action = input(
                "Invalid input. Type 'link' to link these transactions or 'skip' to continue without linking: ")
        if action == "link":
            transaction_id = next_transaction_id()
            update_transaction_id(source_table, source['ID'], transaction_id)
            link_multiple(target_table, potentials[0], transaction_id)
            print("\nTransactions linked successfully\n\n")
    elif len(potentials) > 1:
        print("I didn't find a single entry to match this transaction, but did find sets of entries that sum to the" +
              "desired amount. Source transaction:")
        print(source)
        print("The following sets of entries sum to the desired amount. You can choose to link one of the sets or skip")
        for i in range(len(potentials)):
            summing_entries = potentials[i]
            opt = i + 1
            print("Option {}:".format(opt))
            for entry in summing_entries:
                print(entry)
            print()
        action = input("Type an option number to link those entries or 'skip' to continue without linking: ")
        while not multiple_sums_input_is_valid(action, len(potentials)):
            action = input("Invalid input. Type an option number to link those entries or 'skip' to continue without " +
                           "linking: ")
        if action != "skip":
            idx = int(action) - 1
            transaction_id = next_transaction_id()
            update_transaction_id(source_table, source['ID'], transaction_id)
            link_multiple(target_table, potentials[idx], transaction_id)

    else:
        if not ignore_missing:
            logger.info("No entry found for the following transaction in {table}:\n{entry}, press any key to continue", table=source_table, entry=source)
            input()


def process_matches(source, matches, source_table, singles_only, ignore_missing=False):
    if (len(matches) == 1):
        process_single_match(source, matches.iloc[0])
    elif len(matches) > 1:
        if not singles_only:
            process_multiple_matches(source, matches, source_table=source_table)
    else:
        if not singles_only:
            process_no_matches(source, source_table=source_table, ignore_missing=ignore_missing)

def unlink_all_transactions():
    institution_statement = "UPDATE Institution SET `TransactionID` = NULL;"
    gnucash_statement = "UPDATE Gnucash SET `TransactionID` = NULL;"
    with engine.connect() as conn:
        conn.execute(institution_statement)
        conn.execute(gnucash_statement)

def reconcile(exact_only, reset_transactions):
    logger.remove()
    logger.add(sys.stderr, format="{time} {level} {message}", level="DEBUG")
    if reset_transactions:
        logger.info("unlinking all transactions")
        unlink_all_transactions()

     # pass through gnucash
    logger.info("Searching Gnucash table for unmatched entries")
    for idx, gc_entry in get_unmatched_gnucash().iterrows():
        matches = find_identical(gc_entry, target_table="Institution")
        process_matches(gc_entry, matches, "Gnucash", exact_only, ignore_missing=True)
    # pass through institution
    logger.info("Searching Chase for unmatched entries")
    for idx, inst_entry in get_unmatched_institution().iterrows():
        matches = find_identical(inst_entry, target_table="Gnucash")
        process_matches(inst_entry, matches, "Institution", exact_only, ignore_missing=True)

    # pass through gnucash
    logger.info("Searching Gnucash table for unmatched entries")
    for idx, gc_entry in get_unmatched_gnucash().iterrows():
        matches = find_identical(gc_entry, target_table="Institution")
        process_matches(gc_entry, matches, "Gnucash", exact_only)
    # pass through institution
    logger.info("Searching Chase for unmatched entries")
    for idx, inst_entry in get_unmatched_institution().iterrows():
        matches = find_identical(inst_entry, target_table="Gnucash")
        process_matches(inst_entry, matches, "Institution", exact_only)

    logger.info("Done reconciling")