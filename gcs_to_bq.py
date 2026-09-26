from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DecimalType, TimestampType
)
from pyspark.sql import functions as F



spark = (
    SparkSession.builder
    .appName("BankInterleavedFileToBigQuery")
    .getOrCreate()
)



SOURCE_PATH = (
    "gs://hackathon_noida/pulkitkapoor/usecase/source_data/BankEvalDB_Day0_Interleaved.txt"
)

PROJECT_ID = "dbs-data-ai-ai-core"
DATASET = "pulkit"


TABLE_METADATA = {

    "BRANCHES": {
        "target_table": "shw_branches",
        "schema": StructType([
            StructField("BranchID", IntegerType(), True),
            StructField("BranchName", StringType(), True),
            StructField("Town", StringType(), True),
            StructField("County", StringType(), True),
            StructField("Region", StringType(), True),
            StructField("SortCodePrefix", StringType(), True),
            StructField("OpenDate", TimestampType(), True)
        ])
    },

    "EMPLOYEES": {
        "target_table": "shw_employees",
        "schema": StructType([
            StructField("EmployeeID", IntegerType(), True),
            StructField("BranchID", IntegerType(), True),
            StructField("FirstName", StringType(), True),
            StructField("LastName", StringType(), True),
            StructField("JobTitle", StringType(), True),
            StructField("HireDate", TimestampType(), True),
            StructField("ManagerID", IntegerType(), True)
        ])
    },

    "CUSTOMERS": {
        "target_table": "shw_customers",
        "schema": StructType([
            StructField("CustomerID", IntegerType(), True),
            StructField("FirstName", StringType(), True),
            StructField("LastName", StringType(), True),
            StructField("DateOfBirth", TimestampType(), True),
            StructField("NationalInsuranceNumber", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("Phone", StringType(), True),
            StructField("AddressLine1", StringType(), True),
            StructField("Town", StringType(), True),
            StructField("County", StringType(), True),
            StructField("PostCode", StringType(), True),
            StructField("CustomerSegment", StringType(), True),
            StructField("KYCStatus", StringType(), True),
            StructField("CreatedDate", TimestampType(), True)
        ])
    },

    "ACCOUNTTYPES": {
        "target_table": "shw_accounttypes",
        "schema": StructType([
            StructField("AccountTypeID", IntegerType(), True),
            StructField("TypeName", StringType(), True),
            StructField("InterestRate", DecimalType(18, 6), True),
            StructField("MinBalance", DecimalType(18, 2), True),
            StructField("OverdraftLimit", DecimalType(18, 2), True)
        ])
    },

    "ACCOUNTS": {
        "target_table": "shw_accounts",
        "schema": StructType([
            StructField("AccountID", IntegerType(), True),
            StructField("CustomerID", IntegerType(), True),
            StructField("BranchID", IntegerType(), True),
            StructField("AccountTypeID", IntegerType(), True),
            StructField("SortCode", StringType(), True),
            StructField("AccountNumber", StringType(), True),
            StructField("OpenDate", TimestampType(), True),
            StructField("CloseDate", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("CurrentBalance", DecimalType(18, 2), True),
            StructField("Currency", StringType(), True)
        ])
    },

    "CARDS": {
        "target_table": "shw_cards",
        "schema": StructType([
            StructField("CardID", IntegerType(), True),
            StructField("AccountID", IntegerType(), True),
            StructField("CardNumberMasked", StringType(), True),
            StructField("CardType", StringType(), True),
            StructField("IssueDate", TimestampType(), True),
            StructField("ExpiryDate", TimestampType(), True),
            StructField("Status", StringType(), True)
        ])
    },

    "TRANSACTIONTYPES": {
        "target_table": "shw_transactiontypes",
        "schema": StructType([
            StructField("TransactionTypeID", IntegerType(), True),
            StructField("TypeName", StringType(), True),
            StructField("Category", StringType(), True)
        ])
    },

    "TRANSACTIONS": {
        "target_table": "shw_transactions",
        "schema": StructType([
            StructField("TransactionID", IntegerType(), True),
            StructField("AccountID", IntegerType(), True),
            StructField("TransactionTypeID", IntegerType(), True),
            StructField("TransactionDateTime", TimestampType(), True),
            StructField("Amount", DecimalType(18, 2), True),
            StructField("Currency", StringType(), True),
            StructField("Channel", StringType(), True),
            StructField("MerchantCategory", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("BalanceAfter", DecimalType(18, 2), True),
            StructField("EmployeeID", IntegerType(), True)
        ])
    },

    "LOANS": {
        "target_table": "shw_loans",
        "schema": StructType([
            StructField("LoanID", IntegerType(), True),
            StructField("CustomerID", IntegerType(), True),
            StructField("BranchID", IntegerType(), True),
            StructField("LoanType", StringType(), True),
            StructField("PrincipalAmount", DecimalType(18, 2), True),
            StructField("InterestRate", DecimalType(18, 3), True),
            StructField("TermMonths", IntegerType(), True),
            StructField("StartDate", TimestampType(), True),
            StructField("Status", StringType(), True)
        ])
    },

    "LOANPAYMENTS": {
        "target_table": "shw_loanpayments",
        "schema": StructType([
            StructField("PaymentID", IntegerType(), True),
            StructField("LoanID", IntegerType(), True),
            StructField("DueDate", TimestampType(), True),
            StructField("PaymentDate", TimestampType(), True),
            StructField("AmountDue", DecimalType(18, 2), True),
            StructField("AmountPaid", DecimalType(18, 2), True),
            StructField("PaymentStatus", StringType(), True)
        ])
    }
}


# ============================================================
# 4. READ FILE FROM GCS
# ============================================================

print("Reading source file...")

lines = spark.sparkContext.textFile(SOURCE_PATH).collect()

print("Total lines:", len(lines))


# ============================================================
# 5. PARSE THE INTERLEAVED FILE
# ============================================================

tables = {}

current_table = None
current_header = None

for line in lines:

    line = line.rstrip("\r")

    # --------------------------------------------------------
    # HDR identifies a new table
    # --------------------------------------------------------

    if line.startswith("HDR|"):

        current_table = line.split("|", 1)[1]

        if current_table in TABLE_METADATA:
            current_header = None
            tables[current_table] = []

        continue


    # --------------------------------------------------------
    # FTR is footer information
    # --------------------------------------------------------

    if line.startswith("FTR|"):
        continue


    # --------------------------------------------------------
    # Ignore unknown sections
    # --------------------------------------------------------

    if current_table not in TABLE_METADATA:
        continue


    # --------------------------------------------------------
    # First line after HDR is the column header
    # --------------------------------------------------------

    if current_header is None:

        current_header = line.split("|")

        continue


    # --------------------------------------------------------
    # Store data record
    # --------------------------------------------------------

    tables[current_table].append(line)


# ============================================================
# 6. FUNCTION TO CONVERT STRING DATA
# ============================================================

def convert_dataframe(df, schema):

    for field in schema.fields:

        column_name = field.name
        data_type = field.dataType

        # Empty strings → NULL
        df = df.withColumn(
            column_name,
            F.when(
                F.trim(F.col(column_name)) == "",
                None
            ).otherwise(F.col(column_name))
        )

        if isinstance(data_type, IntegerType):

            df = df.withColumn(
                column_name,
                F.col(column_name).cast("int")
            )

        elif isinstance(data_type, DecimalType):

            df = df.withColumn(
                column_name,
                F.col(column_name).cast(data_type)
            )

        elif isinstance(data_type, TimestampType):

            df = df.withColumn(
                column_name,
                F.to_timestamp(F.col(column_name))
            )

        else:

            df = df.withColumn(
                column_name,
                F.col(column_name).cast("string")
            )

    return df


# ============================================================
# 7. PROCESS EACH TABLE
# ============================================================

for table_name, rows in tables.items():

    print("=" * 60)
    print("Processing table:", table_name)

    metadata = TABLE_METADATA[table_name]

    target_table = metadata["target_table"]
    schema = metadata["schema"]

    if len(rows) == 0:

        print("No records found for:", table_name)

        continue


    # --------------------------------------------------------
    # Create DataFrame as strings first
    # --------------------------------------------------------

    raw_df = spark.read \
        .option("delimiter", "|") \
        .option("header", "false") \
        .csv(
            spark.sparkContext.parallelize(rows)
        )


    # --------------------------------------------------------
    # Rename columns according to metadata
    # --------------------------------------------------------

    column_names = [
        field.name for field in schema.fields
    ]

    for i, column_name in enumerate(column_names):

        raw_df = raw_df.withColumnRenamed(
            f"_c{i}",
            column_name
        )


    # --------------------------------------------------------
    # Convert data types
    # --------------------------------------------------------

    df = convert_dataframe(
        raw_df,
        schema
    )


    # --------------------------------------------------------
    # Select columns in correct order
    # --------------------------------------------------------

    df = df.select(column_names)


    # --------------------------------------------------------
    # Display records
    # --------------------------------------------------------

    print("Records:", df.count())

    df.show(5, truncate=False)


    # ========================================================
    # 8. WRITE TO BIGQUERY
    # ========================================================

    bigquery_table = (
        f"{PROJECT_ID}.{DATASET}.{target_table}"
    )

    print(
        "Writing to BigQuery:",
        bigquery_table
    )

    (
        df.write
        .format("bigquery")
        .option("table", bigquery_table)
        .mode("overwrite")
        .save()
    )

    print(
        "Successfully loaded:",
        bigquery_table
    )


# ============================================================
# 9. FINISH
# ============================================================

print("All tables processed successfully.")

spark.stop()
