import params
from sqlalchemy import create_engine

sources = "'credit','checking','gnucash'"

engine = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}" \
    .format(user=params.db_user, password=params.db_pass, host=params.db_host, port=params.db_port, db=params.db_name))

gnucash_statement = """CREATE TABLE `Gnucash` (
  `ID` int NOT NULL AUTO_INCREMENT,
  `Date` date NOT NULL,
  `Description` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `Merchant` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Amount` decimal(10,2) NOT NULL,
  `AccountID` int NOT NULL,
  `TransactionID` int DEFAULT NULL,
  `BatchID` int NOT NULL,
  `Created` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`ID`)
);"""

institution_statement = """CREATE TABLE `Institution` (
  `ID` int NOT NULL AUTO_INCREMENT,
  `Date` date NOT NULL,
  `Description` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `Merchant` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Amount` decimal(10,2) NOT NULL,
  `AccountID` int NOT NULL,
  `TransactionID` int DEFAULT NULL,
  `PostingDate` date NOT NULL,
  `TransactionDate` date DEFAULT NULL,
  `BatchID` int NOT NULL,
  `Created` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`ID`)
);"""

accounts_statement = """CREATE TABLE `Accounts` (
  `ID` int NOT NULL AUTO_INCREMENT,
  `Description` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL UNIQUE,
  `GnucashName` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `Mapper` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `Created` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`ID`)
);"""

batch_statement = """CREATE TABLE `Batch` (
  `ID` int NOT NULL AUTO_INCREMENT,
  `StartDate` date NOT NULL,
  `EndDate` date NOT NULL,
  `Source` enum('Gnucash','Institution') COLLATE utf8mb4_unicode_ci NOT NULL,
  `AccountID` int DEFAULT NULL,
  `Created` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`ID`)
);"""

sequence_statement = """CREATE TABLE `TransactionIDSequence` (
  `TransactionID` int NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`TransactionID`)
);"""


def create_tables(drop=False, force=False):
  if drop and not force:
    if input("Drop flag will ERASE ALL EXISTING DATA. Type 'yes' to continue: ") != "yes":
      return

  with engine.connect() as conn:
      if drop:
          conn.execute("DROP TABLE IF EXISTS `Gnucash`;")
          conn.execute("DROP TABLE IF EXISTS `Institution`;")
          conn.execute("DROP TABLE IF EXISTS `Accounts`;")
          conn.execute("DROP TABLE IF EXISTS `TransactionIDSequence`;")
          conn.execute("DROP TABLE IF EXISTS `Batch`;")
      conn.execute(gnucash_statement)
      conn.execute(institution_statement)
      conn.execute(accounts_statement)
      conn.execute(sequence_statement)
      conn.execute(batch_statement)