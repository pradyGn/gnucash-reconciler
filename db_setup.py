import params
from sqlalchemy import create_engine

accounts = str(params.accounts_map.values()).split('[')[1].split(']')[0].replace("', '", "','")
sources = "'credit','checking','gnucash'"

engine = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{db}" \
    .format(user=params.db_user, password=params.db_pass, host=params.db_host, port=params.db_port, db=params.db_name))

gnucash_statement = """CREATE TABLE `Gnucash` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `Date` date NOT NULL,
  `Description` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `Merchant` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Amount` decimal(10,2) NOT NULL,
  `Account` enum({accounts}) COLLATE utf8mb4_unicode_ci NOT NULL,
  `TransactionID` int(11) DEFAULT NULL,
  `BatchID` int(11) NOT NULL,
  `Created` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`ID`)
);""".format(accounts=accounts)

institution_statement = """CREATE TABLE `Institution` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `Date` date NOT NULL,
  `Description` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `Merchant` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Amount` decimal(10,2) NOT NULL,
  `Account` enum({accounts}) COLLATE utf8mb4_unicode_ci NOT NULL,
  `TransactionID` int(11) DEFAULT NULL,
  `PostingDate` date NOT NULL,
  `TransactionDate` date DEFAULT NULL,
  `BatchID` int(11) NOT NULL,
  `Created` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`ID`)
);""".format(accounts=accounts)

datefunc_statement = """
CREATE FUNCTION ValidateBatchDateRange(
    batchId int,
    source enum({sources}),
    dateStart datetime,
    dateEnd datetime
)
RETURNS bit
READS SQL DATA
BEGIN
    DECLARE valid bit;
    SET valid = 1;
    IF EXISTS(    SELECT *
                      FROM   `Batch`
                      WHERE  batchId = ID
                      AND    dateStart <= StartDate AND EndDate <= dateEnd
                      AND    source = Source) THEN
           SET valid = 0;
    END IF;
    RETURN valid;
END;
""".format(sources=sources)

batch_statement = """CREATE TABLE `Batch` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `StartDate` date NOT NULL,
  `EndDate` date NOT NULL,
  `Source` enum({sources}) COLLATE utf8mb4_unicode_ci NOT NULL,
  `Created` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`ID`)
);""".format(sources=sources)

sequence_statement = """CREATE TABLE `TransactionIDSequence` (
  `TransactionID` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`TransactionID`)
);"""


def create_tables(drop = False):
    with engine.connect() as conn:
        if drop:
            conn.execute("DROP TABLE IF EXISTS `Gnucash`;")
            conn.execute("DROP TABLE IF EXISTS `Institution`;")
            conn.execute("DROP TABLE IF EXISTS `TransactionIDSequence`;")
            conn.execute("DROP TABLE IF EXISTS `Batch`;")
            conn.execute("DROP FUNCTION IF EXISTS `ValidateBatchDateRange`;")
        conn.execute(gnucash_statement)
        conn.execute(institution_statement)
        conn.execute(sequence_statement)
        conn.execute(datefunc_statement)
        conn.execute(batch_statement)