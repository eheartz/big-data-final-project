from neo4j import GraphDatabase
from db_config import *
import csv


driver = get_neo4j_connection()


# Top 30 Most Accident Prone ZipCodes
def accident_prone_zip_export():
    query = """
    MATCH (collision:Collision)-[:RECORDED_AT]->(zip:ZipCode)
    WITH zip.zipcode AS zipcode, COUNT(collision) AS numCollisions
    ORDER BY numCollisions DESC
    LIMIT 30
    RETURN zipcode, numCollisions
    """
    with driver.session() as session:
        result = session.run(query)
        with open("accident_prone_zip.csv", "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Zipcode", "Number of Collisions"])
            for record in result:
                writer.writerow([record["zipcode"], record["numCollisions"]])


# Boroughs based on safety of pedestrians (amount of pedestrians killed)
def safest_boroughs_export():
    query = """
    MATCH (collision:Collision)-[:ALONG_BOROUGH]->(borough:Borough)
    WITH borough.borough AS borough, SUM(collision.pedestriansKilled) AS totalPedestriansKilled
    ORDER BY totalPedestriansKilled ASC
    LIMIT 5
    RETURN borough, totalPedestriansKilled
    """
    with driver.session() as session:
        result = session.run(query)
        with open("safest_boroughs.csv", "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Borough", "Total Pedestrians Killed"])
            for record in result:
                writer.writerow([record["borough"], record["totalPedestriansKilled"]])


def deadliest_days_2021_export():
    query = """
    MATCH (collision:Collision)-[:ON_DATE]->(date:Date)
    WITH date.date AS date, 
         SUM(collision.personsKilled + collision.pedestriansKilled + collision.cyclistsKilled + collision.motoristsKilled) AS totalKilled
    ORDER BY totalKilled DESC
    LIMIT 10
    RETURN date, totalKilled
    """
    with driver.session() as session:
        result = session.run(query)
        with open("deadliest_days.csv", "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Date", "Total Killed"])
            for record in result:
                writer.writerow([record["date"], record["totalKilled"]])


accident_prone_zip_export()
safest_boroughs_export()
deadliest_days_2021_export()


# Close the driver
driver.close()
