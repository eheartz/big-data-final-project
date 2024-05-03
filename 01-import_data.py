from neo4j import GraphDatabase
from db_config import *

driver = get_neo4j_connection()
csv_file_path = "file:///Motor_Vehicle_Collisions_-_Crashes_20240429.csv"


# init query
def init_data(tx):
    query = (
        f"LOAD CSV WITH HEADERS FROM '{csv_file_path}' AS row "
        "WITH row SKIP 0 LIMIT 300000 "  # Limit to 100k rows.
        "WHERE row.`ZIP CODE` IS NOT NULL "
        "MERGE (zip:ZipCode {zipcode: row.`ZIP CODE`}) "
        "MERGE (date:Date {date: apoc.date.format(apoc.date.parse(row.`CRASH DATE`, 'ms', 'MM/dd/yyyy'), 'ms', 'yyyy-MM-dd')}) "
        "WITH row "
        "WHERE row.BOROUGH IS NOT NULL "
        "MERGE (loc:Borough {borough: row.BOROUGH}) "
        "WITH row "
        "WHERE row.`ON STREET NAME` IS NOT NULL "
        "MERGE (st:Street {streetName: row.`ON STREET NAME`})"
    )
    tx.run(query)


# main node query
def collision_data(tx):
    query = (
        f"LOAD CSV WITH HEADERS FROM '{csv_file_path}' AS row "
        "WITH row SKIP 0 LIMIT 100000 "
        "WHERE row.`ZIP CODE` IS NOT NULL "
        "MERGE (collision:Collision {"
        "    crashTime: row.`CRASH TIME`,"
        "    personsKilled: toInteger(row.`NUMBER OF PERSONS KILLED`),"
        "    pedestriansKilled: toInteger(row.`NUMBER OF PEDESTRIANS KILLED`),"
        "    cyclistsKilled: toInteger(row.`NUMBER OF CYCLIST KILLED`),"
        "    motoristsKilled: toInteger(row.`NUMBER OF MOTORIST KILLED`),"
        "    locationZip: toInteger(row.`ZIP CODE`),"
        "    locationBorough: row.BOROUGH,"
        "    crashDate: date(datetime({epochmillis: apoc.date.parse(row.`CRASH DATE`, 'ms', 'MM/dd/yyyy')}))"
        "}) "
        "MERGE (borough:Borough {borough: row.BOROUGH}) "
        "MERGE (zip:ZipCode {zipcode: row.`ZIP CODE`}) "
        "MERGE (collision)-[:RECORDED_AT]->(zip) "
        "MERGE (date:Date {date: toString(collision.crashDate)}) "
        "MERGE (collision)-[:ON_DATE]->(date) "
        "MERGE (collision)-[:AT_TIME]->(time:Time {HHMM: row.`CRASH TIME`}) "
        "MERGE (collision)-[:ALONG_BOROUGH]->(borough)"
    )
    tx.run(query)


def delete_data(tx):
    query = """
      CALL {
      MATCH (n)
      DETACH DELETE n
      }"""
    tx.run(query)


with driver.session() as session:
    results = session.execute_write(delete_data)
    print("I deleted the data")
    results = session.execute_write(init_data)
    print("I executed the init_data query")
    results = session.execute_write(collision_data)
    print("I executed the collision creation query")


# Close the driver
driver.close()
