from pathlib import Path
from zipfile import ZipFile

from boto3 import client

import os
import sys
import threading
import pandas as pd
import click


class ProgressPercentage:
    def __init__(self, filename):
        self._filename = filename
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += (bytes_amount / 1024) / 1024
            sys.stdout.write("\r%s  %.2f MB" % (self._filename, self._seen_so_far))
            sys.stdout.flush()


# dev/output_files/mce_macro_dev.zip


@click.command("fetch-output")
@click.option(
    "-b",
    "--bucket",
    type=click.STRING,
    default="data.canvas.thought.spot.generic.upload.cisco.com",
)
@click.option("-f", "--folder", type=click.STRING, default="dev/output_files")
@click.option("-k", "--key", type=click.STRING, default="mce_macro_dev.zip")
@click.option(
    "-dl",
    "--download-location",
    type=click.Path(path_type=Path),
    default=(Path("~").expanduser() / "Downloads" / "mce_macro_dev.zip"),
    required=True,
)
@click.option("-d", "--data-location", type=click.Path(path_type=Path), required=True)
def fetch_s3_output(bucket, folder, key, download_location, data_location):
    if download_location.exists():
        download_location.unlink()
    download_location = str(download_location)
    object_key = f"{folder}/{key}"
    s3 = client("s3")
    s3.download_file(
        bucket,
        object_key,
        download_location,
        Callback=ProgressPercentage(download_location),
    )
    download_location = Path(download_location)
    if download_location.suffix == ".zip":
        frames = []
        with ZipFile(download_location, "r") as zip_ref:
            for file in zip_ref.namelist():
                if file.endswith(".csv"):
                    frames.append(pd.read_csv(zip_ref.open(file), low_memory=False))
        df = pd.concat(frames)
    else:
        df = pd.read_excel(download_location)

    df.to_csv(data_location, index=False)


if __name__ == "__main__":
    fetch_s3_output()
