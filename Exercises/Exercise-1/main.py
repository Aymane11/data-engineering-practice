import requests
import zipfile
import os
import aiohttp
from loguru import logger

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


async def async_download_file(uri: str) -> str:
    filename = uri.split("/")[-1]
    logger.info(f"Downloading {filename}")
    async with aiohttp.ClientSession() as session:
        async with session.get(uri) as resp:
            with open(filename, "wb") as f:
                f.write(await resp.read())
    return filename


def download_file(uri: str, destination_dir: str) -> str:
    filename = uri.split("/")[-1]
    filepath = os.path.join(destination_dir, filename)
    logger.info(f"Downloading to {filepath}")
    r = requests.get(uri)
    with open(filepath, "wb") as f:
        f.write(r.content)
    return filepath


def unzip_file(filename: str, destination_dir: str) -> None:
    logger.info(f"Unzipping {filename}")
    try:
        with zipfile.ZipFile(filename, "r") as zip_ref:
            zip_ref.extractall(destination_dir)
    except zipfile.BadZipFile:
        logger.error(f"File {filename} is not good zipfile")


def remove_file(filename: str) -> None:
    logger.info(f"Removing {filename}")
    os.remove(filename)


def create_directory(directory: str) -> None:
    logger.info(f"Creating {directory}")
    if not os.path.exists(directory):
        os.makedirs(directory)


def main():
    DOWN_DIR = "downloads"
    create_directory(DOWN_DIR)
    for uri in download_uris:
        filename = download_file(uri, DOWN_DIR)
        unzip_file(filename, DOWN_DIR)
        remove_file(filename)


if __name__ == "__main__":
    main()
