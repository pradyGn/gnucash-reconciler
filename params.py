with open("db_access.txt", 'r') as f:
	db_user, db_pass = f.read().split("\n")

db_name = "finance"
db_host = "localhost"
db_port = 3306
# accounts_map = {"Checking Account": "Checking", "Credit Card": "FreedomCredit"}
gnucash_name = 'Gnucash'
checking_name = 'ChaseChecking'
credit_name = 'FreedomCredit'
date_threshold = 4
max_combinations = 5_000_000
max_combination_size = 10