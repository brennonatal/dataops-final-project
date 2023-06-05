import logging
import os
import re

import mysql.connector
import pandas as pd
import requests
from bs4 import BeautifulSoup

DATABASE_IP = os.getenv("DATABASE_IP")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")


def scrape_this(uri="/pages/forms/"):
    page = requests.get("https://scrapethissite.com" + uri)
    soup = BeautifulSoup(page.text, "html.parser")

    div = soup.find(id="hockey")
    table = div.find("table")

    data_rows = table.find_all("tr", attrs={"class": "team"})
    parsed_data = list()
    stat_keys = [col.attrs["class"][0] for col in data_rows[0].find_all("td")]

    for row in data_rows:
        tmp_data = dict()
        for attr in stat_keys:
            attr_val = row.find(attrs={"class": attr}).text
            tmp_data[attr] = re.sub(r"^\s+|\s+$", "", attr_val)
        parsed_data.append(tmp_data)

    data_df = pd.DataFrame(parsed_data)
    return data_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

    logging.info("Scraping data from https://scrapethissite.com/pages/forms/")
    page = requests.get("https://scrapethissite.com/pages/forms/")
    soup = BeautifulSoup(page.text, "html.parser")
    pagination = soup.find("ul", attrs={"class": "pagination"})
    link_elms = pagination.find_all("li")
    links = [link_elm.find("a").attrs["href"] for link_elm in link_elms]
    links = set(links)

    temp_dfs = list()
    for link in links:
        tmp_df = scrape_this(uri=link)
        temp_dfs.append(tmp_df)
    hockey_team_df = pd.concat(temp_dfs, axis=0).reset_index()
    hockey_team_df.sort_values(["year", "name"], inplace=True)
    logging.info("Data scraped successfully")

    try:
        # Connect to MySQL
        logging.info("Connecting to MySQL")
        mydb = mysql.connector.connect(
            host=DATABASE_IP,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD,
            database="hockey",
        )

        # Create cursor
        mycursor = mydb.cursor()
        logging.info("Connected to MySQL")

        # Create table if not exists
        mycursor.execute("""
          CREATE TABLE IF NOT EXISTS hockey_stats (
          id INT NOT NULL,
          name VARCHAR(255),
          year INT,
          wins INT,
          losses INT,
          ot_losses INT,
          pct FLOAT,
          gf INT,
          ga INT,
          diff INT,
          PRIMARY KEY (id)
      );
      """)
        mydb.commit()

        # Insert data
        logging.info("Inserting data into MySQL")
        for _, row in hockey_team_df.iterrows():
            query = f"""
              INSERT INTO hockey_stats (id, name, year, wins, losses, ot_losses, pct, gf, ga, diff)
              VALUES (
                {row["index"] + 1}, 
                '{row["name"]}', 
                {row["year"]}, 
                {row["wins"]}, 
                {row["losses"]}, 
                {row["ot-losses"] or 'NULL'}, 
                {row["pct"]}, 
                {row["gf"]}, 
                {row["ga"]}, 
                {row["diff"]}
                )
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                year = VALUES(year),
                wins = VALUES(wins),
                losses = VALUES(losses),
                ot_losses = VALUES(ot_losses),
                pct = VALUES(pct),
                gf = VALUES(gf),
                ga = VALUES(ga),
                diff = VALUES(diff)
                """
            mycursor.execute(query)

        mydb.commit()
        logging.info("Data inserted successfully")

    except mysql.connector.Error as error:
        print("Failed to insert data into MySQL table {}".format(error), query)

    finally:
        if mydb.is_connected():
            mycursor.close()
            mydb.close()
            print("MySQL connection is closed")
