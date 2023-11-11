import requests
import pandas


URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
TARGET_DATE = "2022-02-07 14:03"


def main():
    scraped_df = pandas.read_html(URL)[0]
    filename = scraped_df[scraped_df["Last modified"] == TARGET_DATE].iloc[0]["Name"]
    df = pandas.read_csv(URL + filename)
    print(df[df["HourlyDryBulbTemperature"] == df["HourlyDryBulbTemperature"].max()])


if __name__ == "__main__":
    main()
