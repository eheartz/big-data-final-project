from neo4j import GraphDatabase
from db_config import *

driver = get_neo4j_connection()
csv_file_path =  "file:///Motor_Vehicle_Collisions_-_Crashes_20240429.csv"


# Top 15 Most Accident Prone ZipCodes
def accident_prone_zip(tx):
    query = """
    MATCH (collision:Collision)-[:RECORDED_AT]->(zip:ZipCode)
    WITH zip.zipcode AS zipcode, COUNT(collision) AS numCollisions
    ORDER BY numCollisions DESC
    LIMIT 15
    RETURN zipcode, numCollisions
    """
    result = tx.run(query)
    return result

# Boroughs based on safety of pedestrians (amount of pedestrians killed)
def safest_boroughs(tx):
    query = """
    MATCH (collision:Collision)-[:ALONG_BOROUGH]->(borough:Borough)
    WITH borough.borough AS borough, SUM(collision.pedestriansKilled) AS totalPedestriansKilled
    ORDER BY totalPedestriansKilled ASC
    LIMIT 5
    RETURN borough, totalPedestriansKilled
    """
    result = tx.run(query)
    return result    

def yearly_deaths(tx):
    query = """
    MATCH (collision:Collision)
    WITH substring(toString(collision.crashDate), 0, 4) AS year, 
         SUM(collision.pedestriansKilled + collision.cyclistsKilled + collision.motoristsKilled + collision.personsKilled) AS totalDeaths
    RETURN year, totalDeaths
    ORDER BY year DESC
    """
    result = tx.run(query)
    return result


with driver.session() as session:
    # Query 1
    result = accident_prone_zip(session)
    if result:
        print("Top 15 most accident-prone zip codes:")
        for record in result:
            print(f"Zip code: {record['zipcode']} (Total accidents: {record['numCollisions']})")
    else:
        print("No data found.")
    result = safest_boroughs(session)
    # Query 2
    if result:
        print("Top 5 safest boroughs for pedestrians:")
        for record in result:
            print(f"Borough: {record['borough']} (Total pedestrians killed: {record['totalPedestriansKilled']})")
    else:
        print("No data found.")
    # Query 3
    result = yearly_deaths(session)
    if result:
        print("The total number of deaths for each year:")
        for record in result:
            print(f"Year: {record['year']} (Total deaths: {record['totalDeaths']})")
    else:
        print("No data found.")
        
# Close the driver
driver.close()



# Close the driver
driver.close()