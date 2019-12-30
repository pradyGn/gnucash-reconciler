# gnucash-reconciler
Tool for comparing Gnucash records to bank records automatically. This CLI can be used to import Gnucash and Chase Bank CSV files containing transactions to check for discrepancies.

## Usage
You will need a MySQL server running. Create a file called "db_access.txt" with a username and password separated by a line break. These should have write access to the database specified in `params.py` ("finance" by default).

#### Setup tables
To create the required database tables, run `python3 main.py create-tables`. This will create four tables:
1. Gnucash, to store your Gnucash transactions
2. Institution, to store your institution's records
3. Batch, to store file import metadata
4. TransactionIDSequence, a regular table that acts as a MySQL Sequence to auto increment IDs

#### Import data
The importer is currently configured to allow two overlapping imports to Institution, tagged as "checking" or "credit," each expecting Chase CSV formatted records. The data expected from Gnucash can be obtained with the "Export transactions to CSV..." dialog. If Gnucash transactions are recorded in the "Description (Merchant)" format, these fields will be broken out. Otherwise, the full description will be duplicated across both fields.
Use `python3 main.py import -h` for a help message on parameters. Specify a source (gnucash/checking/credit), file path, and date range covered by the import.

#### Reconcile transactions
Reconciliation is performed by searching the Institution and Gnucash tables for identical entries. When transactions are linked, a new TransactionID is generated and this is recorded in both tables. This allows one-to-many linking to allow purchases to be broken out into smaller transactions. The script will look within the date threshold specified in params.py to find transactions with identical amounts. If only one is found, they will be automatically linked and generate a debug message like this:
```
2019-12-29T20:02:19.434406-0500 DEBUG Matched Gnucash 136 (Taxi on 2019-01-06) to Institution 211, Transaction ID 119
```
If multiple transactions with the correct amounts are found, the script will put forth a prompt like this:
```
I found 2 matches. Source: 
ID                               149
Date                      2019-01-11
Description                     Taxi
Merchant                        Taxi
Amount                        -14.76
Account                FreedomCredit
TransactionID                   None
BatchID                            1
Created          2019-12-29 20:02:01
Name: 148, dtype: object
Matches:
    ID        Date             Description Merchant  Amount        Account TransactionID PostingDate TransactionDate  BatchID             Created
0  189  2019-01-13         TAXI SVC QUEENS     None  -14.76  FreedomCredit          None  2019-01-15      2019-01-13        2 2019-12-29 20:02:04
1  200  2019-01-11  TAXI SVC LONG ISLAND C     None  -14.76  FreedomCredit          None  2019-01-13      2019-01-11        2 2019-12-29 20:02:04
Enter the ID of the correct transaction to attach, or 'skip' to continue: 
```
In the above example, the correct response would be `200` since these dates match.

After a pass looking for exact amount matches, the script will attempt one-to-many linkings. This will result in a prompt like the following:
```
I didn't find a single entry to match, but did find one set of entries that sum to the desired amount. Source transaction:
ID                                                               277
Date                                                      2019-02-18
Description        NON-CHASE ATM WITHDRAW               405075  0...
Merchant                                                        None
Amount                                                          -143
Account                                                     Checking
TransactionID                                                   None
PostingDate                                               2019-02-19
TransactionDate                                           2019-02-18
BatchID                                                            3
Created                                          2019-12-29 20:02:06
Name: 30, dtype: object
Summing transactions:
[ID                                61
Date                      2019-02-18
Description           ATM Withdrawal
Merchant             Bank of America
Amount                          -140
Account                     Checking
TransactionID                   None
BatchID                            1
Created          2019-12-29 20:02:01
Name: 0, dtype: object, ID                                62
Date                      2019-02-18
Description                  ATM Fee
Merchant             Bank of America
Amount                            -3
Account                     Checking
TransactionID                   None
BatchID                            1
Created          2019-12-29 20:02:01
Name: 1, dtype: object]
Type 'link' to link these entries into a transaction or 'skip' to continue without linking: 
```
If several matching sets are found, they will each be presented for choice.

If no matches can be found, the script will notify and pause for input:
```
2019-12-29T20:16:53.248722-0500 INFO No entry found for the following transaction in Gnucash:
ID                               258
Date                      2019-02-24
Description                Groceries
Merchant                 Whole Foods
Amount                        -27.98
Account                FreedomCredit
TransactionID                   None
BatchID                            1
Created          2019-12-29 20:02:01
Name: 24, dtype: object, press any key to continue
```
This allows the user to find the error and correct it.