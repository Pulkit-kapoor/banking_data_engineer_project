from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read-One-BigQuery-Table") \
    .getOrCreate()

print("Spark version:", spark.version)



from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Join-BigQuery-Tables") \
    .getOrCreate()


# SOURCE dataset
SOURCE_DATASET = "pulkit_bank"

# TARGET dataset
TARGET_DATASET = "pulkit_bank_silver"

TABLE_ID1 = "shw_transactions"
TABLE_ID2 = "shw_customers"
TABLE_ID3 = "shw_accounts"

# -----------------------------
# Read transactions
# -----------------------------
table_transactions = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID1}"

df_transactions = spark.read \
    .format("bigquery") \
    .option("table", table_transactions) \
    .load()

print("Transactions loaded successfully!")
df_transactions.printSchema()


# -----------------------------
# Read customers
# -----------------------------
table_customers = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID2}"

df_customers = spark.read \
    .format("bigquery") \
    .option("table", table_customers) \
    .load()

print("Customers loaded successfully!")
df_customers.printSchema()


# -----------------------------
# Read accounts
# -----------------------------
table_accounts = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID3}"

df_accounts = spark.read \
    .format("bigquery") \
    .option("table", table_accounts) \
    .load()

print("Accounts loaded successfully!")
df_accounts.printSchema()



print(type(df_transactions))
print(type(df_accounts))
print(type(df_customers))


t = df_transactions.alias("t")
a = df_accounts.alias("a")
c = df_customers.alias("c")

df_final2 = (
    t.join(
        a,
        t["AccountID"] == a["AccountID"],
        "inner"
    )
    .join(
        c,
        a["CustomerID"] == c["CustomerID"],
        "inner"
    )
    .select(
        # Transaction
        t["TransactionID"],
        t["TransactionTypeID"],
        t["TransactionDateTime"],
        t["Amount"],
        t["Currency"].alias("TransactionCurrency"),
        t["Channel"],
        t["MerchantCategory"],
        t["Description"],
        t["BalanceAfter"],
        t["EmployeeID"],

        # Account
        a["AccountID"],
        a["BranchID"],
        a["AccountTypeID"],
        a["SortCode"],
        a["AccountNumber"],
        a["OpenDate"],
        a["CloseDate"],
        a["Status"].alias("AccountStatus"),
        a["CurrentBalance"].alias("AccountCurrentBalance"),
        a["Currency"].alias("AccountCurrency"),

        # Customer
        c["CustomerID"],
        c["FirstName"],
        c["LastName"],
        c["DateOfBirth"],
        c["NationalInsuranceNumber"],
        c["Email"],
        c["Phone"],
        c["AddressLine1"],
        c["Town"],
        c["County"],
        c["PostCode"],
        c["CustomerSegment"],
        c["KYCStatus"],
        c["CreatedDate"]
    )
)

print("All three tables joined successfully!")



#output

OUTPUT_TABLE = (
    f"{PROJECT_ID}.{TARGET_DATASET}."
    "customer_transaction_details"
)

df_final2.write \
    .format("bigquery") \
    .option("table", OUTPUT_TABLE) \
    .mode("overwrite") \
    .save()

print("Successfully written to:")
print(OUTPUT_TABLE)

