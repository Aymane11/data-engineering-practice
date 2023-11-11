from token import ASYNC
import requests
import zipfile
from pathlib import Path
import aiohttp
from loguru import logger
import asyncio

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


async def download_file(uri: str, destination_dir: Path, async_mode: bool) -> str:
    filename = uri.split("/")[-1]
    filepath = destination_dir / filename
    logger.info(f"Downloading to {filepath}")
    if async_mode:
        async with aiohttp.ClientSession() as session:
            async with session.get(uri) as resp:
                with open(filepath, "wb") as f:
                    f.write(await resp.read())
    else:
        r = requests.get(uri)
        with open(filepath, "wb") as f:
            f.write(r.content)
    return filepath


def unzip_file(file: Path, destination_dir: Path) -> None:
    logger.info(f"Unzipping {file}")
    try:
        with zipfile.ZipFile(file, "r") as zip_ref:
            zip_ref.extractall(destination_dir)
    except zipfile.BadZipFile:
        logger.error(f"File {file} is not good zipfile")


def remove_file(file: Path) -> None:
    logger.info(f"Removing {file}")
    file.unlink()


def create_directory(directory: str) -> Path:
    logger.info(f"Creating {directory}")
    Path(directory).mkdir(parents=True, exist_ok=True)
    return Path(directory)


async def process(uri: str, destination_dir: Path, async_mode: bool) -> None:
    filename = await download_file(uri, destination_dir, async_mode)
    unzip_file(filename, destination_dir)
    remove_file(filename)


if __name__ == "__main__":
    ASYNC_PROCESSING = True
    download_dir = create_directory("downloads")
    if ASYNC_PROCESSING:
        logger.info("Starting async processing")
        loop = asyncio.get_event_loop()
        tasks = [process(uri, download_dir, True) for uri in download_uris]
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()
    else:
        logger.info("Starting sync processing")
        for uri in download_uris:
            asyncio.run(process(uri, download_dir, False))
