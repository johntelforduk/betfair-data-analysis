# Explore, analyse and visualise Betfair historic data using PySpark.

from pyspark.sql import SparkSession        # For Spark.
from pyspark.sql.functions import explode   # Explodes lists into rows.
import bz2file as bz2                       # To decompress the Betfair data files.
import glob2 as glob                        # To scan across folders looking for data files.
import json                                 # To decode Betfair data.
from pyspark.sql.types import *             # For authoring the RDD schema.

# Based on, https://stackoverflow.com/questions/31185812/processing-bzipped-json-file-in-spark
def read_fun_generator(filename):
    with bz2.open(filename, 'rb') as f:
        for line in f:
            # decode - turns binary object into string.
            # json.loads - turns JSON string into Python dictionary.
            yield json.loads(line.strip().decode())

print('Starting Spark.')
spark = SparkSession.builder.appName('BetFair').getOrCreate()
sc = spark.sparkContext
print('Spark started.')


print('Gathering file list.')
filelist = glob.glob('./data/**/*.bz2')
print('Number of files found =', len(filelist))

betfair_rdd = sc.parallelize(filelist).flatMap(read_fun_generator)
# print('betfair_rdd count =', betfair_rdd.count())     # Count is 314252
print('betfair_rdd.take(2) =', betfair_rdd.take(2))


# betfair_rdd_schema = StructType([
#     StructField("op", StringType(), True),
#     StructField("clk", StringType(), True),
#     StructField("pt", IntegerType(), True),
#     StructField("mc", ArrayType(
#         StructType([
#             StructField("id", StringType(), True),
#             StructField("marketDefinition", StructType([
#                 StructField("bspMarket", BooleanType(), True),
#                 StructField("turnInPlayEnabled", BooleanType(), True),
#                 StructField("persistenceEnabled", BooleanType(), True),
#                 StructField("marketBaseRate", FloatType(), True),
#                 StructField("eventId", StringType(), True),
#                 StructField("eventTypeId", StringType(), True),
#                 StructField("numberOfWinners", IntegerType(), True),
#                 StructField("bettingType", StringType(), True),
#                 StructField("marketTime", StringType(), True),
#                 StructField("suspendTime", StringType(), True),
#                 StructField("bspReconciled", BooleanType(), True),
#                 StructField("complete", BooleanType(), True),
#                 StructField("inPlay", BooleanType(), True),
#                 StructField("crossMatching", BooleanType(), True),
#                 StructField("runnersVoidable", BooleanType(), True),
#                 StructField("numberOfActiveRunners", IntegerType(), True),
#                 StructField("betDelay", IntegerType(), True),
#                 StructField("status", StringType(), True),
#
#                 StructField("runners", ArrayType(
#                     StructType([
#                         StructField("status", StringType(), True),
#                         StructField("sortPriority", IntegerType(), True),
#                         StructField("id", StringType(), True),
#                         StructField("name", StringType(), True),
#                     ])
#                 ), True),
#
#                 StructField("regulators", ArrayType(
#                     StringType()
#                 ), True),
#
#                 StructField("countryCode", StringType(), True),
#                 StructField("discountAllowed", BooleanType(), True),
#                 StructField("timezone", StringType(), True),
#                 StructField("openDate", StringType(), True),
#                 StructField("version", IntegerType(), True),
#                 StructField("name", StringType(), True),
#                 StructField("eventName", StringType(), True)
#
#             ]), True),
#             StructField("rc", ArrayType(
#                 StructType([
#                     StructField("ltp", FloatType(), True),
#                     StructField("id", IntegerType(), True)
#                           ])
#                                        ),
#                    True)
#                   ])
#                                ),
#            True)
#                    ])

# It would be nice if Spark could infer the schema for itself, but this doesn't seem to work very well for JSONs
# with nested objects. So, sadly, we have to provide the schema manually.

# TODO Reintroduce the correct type for numbers.
betfair_rdd_schema = StructType([
    StructField('op', StringType(), True),
    StructField('clk', StringType(), True),
    StructField('pt', StringType(), True),
    StructField('mc', ArrayType(
        StructType([
            StructField('id', StringType(), True),
            StructField('marketDefinition', StructType([
                StructField('bspMarket', BooleanType(), True),
                StructField('turnInPlayEnabled', BooleanType(), True),
                StructField('persistenceEnabled', BooleanType(), True),
                StructField('marketBaseRate', StringType(), True),
                StructField('eventId', StringType(), True),
                StructField('eventTypeId', StringType(), True),
                StructField('numberOfWinners', StringType(), True),
                StructField('bettingType', StringType(), True),
                StructField('marketTime', StringType(), True),
                StructField('marketType', StringType(), True),
                StructField('suspendTime', StringType(), True),
                StructField('bspReconciled', BooleanType(), True),
                StructField('complete', BooleanType(), True),
                StructField('inPlay', BooleanType(), True),
                StructField('crossMatching', BooleanType(), True),
                StructField('runnersVoidable', BooleanType(), True),
                StructField('settledTime', StringType(), True),
                StructField('numberOfActiveRunners', StringType(), True),
                StructField('betDelay', StringType(), True),
                StructField('status', StringType(), True),
                StructField('runners', ArrayType(
                    StructType([
                        StructField('status', StringType(), True),
                        StructField('sortPriority', StringType(), True),
                        StructField('id', StringType(), True),
                        StructField('name', StringType(), True),
                    ])
                ), True),
                StructField('regulators', ArrayType(
                    StringType()
                ), True),
                StructField('countryCode', StringType(), True),
                StructField('discountAllowed', BooleanType(), True),
                StructField('timezone', StringType(), True),
                StructField('openDate', StringType(), True),
                StructField('version', StringType(), True),
                StructField('name', StringType(), True),
                StructField('eventName', StringType(), True)
            ]), True),
            StructField('rc', ArrayType(
                StructType([
                    StructField('ltp', StringType(), True),
                    StructField('id', StringType(), True)
                ])
            ), True)
        ])
    ), True)
])

# Convert the RDD into a DataFrame.
betfair_df = spark.createDataFrame(betfair_rdd, betfair_rdd_schema)
betfair_df.createOrReplaceTempView('betfair_rdd')

betfair_df.printSchema()
print('betfair_df.take(10) =', betfair_df.take(10))
# print('betfair_df.count() =', betfair_df.count())

op2 = betfair_df.select('*', explode(betfair_df.mc).alias('mc_row'))
print('op2.take(10) =', op2.take(10))

# Market Definition dataframe is a filter of rows where there is a Market Definition row present.
md1 = op2.filter(op2.mc_row.marketDefinition.isNotNull())
print('md1.take(10) =', md1.take(10))

market_definition = md1.selectExpr('op',
                                   'clk',
                                   'pt',
                                   'mc_row.id AS market_id',
                                   'mc_row.rc AS rc',
                                   'mc_row.marketDefinition.betDelay AS bet_delay',
                                   'mc_row.marketDefinition.bettingType AS betting_type',
                                   'mc_row.marketDefinition.bspMarket AS bsp_market',
                                   'mc_row.marketDefinition.bspReconciled AS bsp_reconciled',
                                   'mc_row.marketDefinition.complete AS complete',
                                   'mc_row.marketDefinition.countryCode AS country_code',
                                   'mc_row.marketDefinition.crossMatching AS cross_matching',
                                   'mc_row.marketDefinition.discountAllowed AS discount_allowed',
                                   'mc_row.marketDefinition.eventId AS event_id',
                                   'mc_row.marketDefinition.eventName AS event_name',
                                   'mc_row.marketDefinition.eventTypeId AS event_type_id',
                                   'mc_row.marketDefinition.inPlay AS in_play',
                                   'mc_row.marketDefinition.marketBaseRate AS market_base_rate',
                                   'mc_row.marketDefinition.marketTime AS market_time',
                                   'mc_row.marketDefinition.marketType AS market_type',
                                   'mc_row.marketDefinition.numberOfActiveRunners AS number_of_active_runners',
                                   'mc_row.marketDefinition.numberOfWinners AS number_of_winners',
                                   'mc_row.marketDefinition.openDate AS open_date',
                                   'mc_row.marketDefinition.persistenceEnabled AS persistence_enabled',
                                   'mc_row.marketDefinition.runnersVoidable AS runners_voidable',
                                   'mc_row.marketDefinition.settledTime AS settled_time',
                                   'mc_row.marketDefinition.status AS status',
                                   'mc_row.marketDefinition.suspendTime AS suspend_time',
                                   'mc_row.marketDefinition.timezone AS timezone',
                                   'mc_row.marketDefinition.turnInPlayEnabled AS turn_in_play_enabled',
                                   'mc_row.marketDefinition.version AS version',
                                   'mc_row.marketDefinition.name AS market_name',
                                   'mc_row.marketDefinition.regulators AS regulators',
                                   'mc_row.marketDefinition.runners AS runners'
                                   )

print('market_definition.take(10) =', market_definition.take(10))

ru1 = market_definition.select(market_definition.op,
                               market_definition.clk,
                               market_definition.pt,
                               market_definition.event_id,
                               market_definition.event_name,
                               explode(market_definition.runners).alias('runner_row'))
print('ru1.take(10) =', ru1.take(10))

runners = ru1.selectExpr('op',
                         'clk',
                         'pt',
                         'event_id',
                         'event_name',
                         'runner_row.id AS runner_id',
                         'runner_row.name AS runner_name',
                         'runner_row.status AS runner_status',
                         'runner_row.sortPriority AS sort_priority'
                         )
print('runners.take(10) =', runners.take(10))

rc1 = market_definition.select(market_definition.op,
                               market_definition.clk,
                               market_definition.pt,
                               market_definition.event_id,
                               market_definition.event_name,
                               explode(market_definition.rc).alias('runner_change_row'))
print('rc1.take(10) =', rc1.take(10))

runner_changes = rc1.selectExpr('op',
                                'clk',
                                'pt',
                                'event_id',
                                'event_name',
                                'runner_change_row.id AS runner_id',
                                'runner_change_row.ltp AS last_traded_price'
                                )
print('runner_changes.take(10) =', runner_changes.take(10))

print()

# usa_elections = market_definition.where("event_id='27938931'")
# print("usa_elections", usa_elections.collect())


some_events = market_definition.select('market_id', 'market_name', 'event_id', 'event_name').distinct().take(1000)
for e in some_events:
    print(e)
# Row(event_id='27938931', event_name='USA - Congressional Elections')

# TODO Make a new df which is runner_changes enriched with info from runners.


print('Stopping Spark.')
spark.stop()
print('Spark closed.')
