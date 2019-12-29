
from sqlalchemy import create_engine
import pandas as pd
import params
from loguru import logger

engine = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}" \
                       .format(user=params.db_user, password=params.db_pass, host=params.db_host, port=params.db_port,
                               db=params.db_name))

def get_batch_entry(batch_id):
    query = "SELECT * FROM Batch WHERE `ID` = {id}".format(id=batch_id)
    return pd.read_sql(query, engine).iloc[0]


def other_table(original_table):
    if original_table == "Gnucash":
        return "Institution"
    elif original_table == "Institution":
        return "Gnucash"
    else:
        raise ValueError("Bad table name {} supplied".format(original_table))


def get_table_to_remove(batch_id):
    query = "SELECT `Source` FROM Batch WHERE `ID` = {batch_id}".format(batch_id=batch_id)
    with engine.connect() as conn:
        source = conn.execute(query).fetchone()['Source']
    if source == "gnucash":
        return "Gnucash"
    else:
        return "Institution"

def unlink_transactions_in_other_table(main_table, linked_table, batch_id):
    transaction_ids_query = "SELECT `TransactionID` FROM {table} WHERE `BatchID` = {batch_id}".format(table=main_table, batch_id=batch_id)
    update_query = "UPDATE {table} SET TransactionID = NULL WHERE TransactionID IN ({transaction_ids_subquery})".format(table=linked_table, transaction_ids_subquery=transaction_ids_query)
    with engine.connect() as conn:
        conn.execute(update_query)
        return True
    return False


def remove_main_entries(main_table, batch_id):
    query = "DELETE FROM {table} WHERE `BatchID` = {batch_id}".format(table=main_table, batch_id=batch_id)
    with engine.connect() as conn:
        conn.execute(query)
        return True
    return False


def remove_transaction_id_entries(main_table, batch_id):
    transaction_ids_query = "SELECT `TransactionID` FROM {table} WHERE `BatchID` = {batch_id}".format(table=main_table, batch_id=batch_id)
    remove_query = "DELETE FROM TransactionIDSequence WHERE TransactionID in ({subquery})".format(subquery=transaction_ids_query)
    with engine.connect() as conn:
        conn.execute(remove_query)
        return True
    return False

def remove_batch_entry(batch_id):
    query = "DELETE FROM Batch WHERE ID = {batch_id}".format(batch_id=batch_id)
    with engine.connect() as conn:
        conn.execute(query)
        return True
    return False


def remove_batch(batch_id):
    # validate
    entry = get_batch_entry(batch_id)
    print(entry)
    response = input("Are you sure you want to remove the entry above? Type 'yes' to continue: ")
    if response != "yes":
        print("Aborting")
        return

    # get tables
    main_table = get_table_to_remove(batch_id)
    linked_table = other_table(main_table)

    # unlink all transaction ids in other table
    if not unlink_transactions_in_other_table(main_table, linked_table, batch_id):
        logger.error("Failed to remove batch")
        return

    # remove transaction id entry
    if not remove_transaction_id_entries(main_table, batch_id):
        logger.error("Failed to remove transaction ID")
        return

    # remove all entries in main table
    if not remove_main_entries(main_table, batch_id):
        logger.error("Failed to remove entries")
        return

    # remove batch entry
    if not remove_batch_entry(batch_id):
        logger.error("Failed to remove batch entry")