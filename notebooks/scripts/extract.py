import os
import polars as pl
from typing import Union
import tarfile
import logging
from .helpers import compose

def to_tar(url: str, dest='../data') -> None:
    tar = tarfile.open(url)
    tar.extractall(dest)
    logging.info(f'extracted tar to {dest}')
    

def to_parquet(url: str) -> Union[pl.DataFrame, str]:
    assert os.path.exists(url), f"path doesn't exist {url}"
    if os.path.splitext(url)[-1] == '.parquet':
        return url

    df = pl.read_csv(url, separator="\t", truncate_ragged_lines=True)
    os.remove(url)

    # create new file
    url = url.split('/')
    dest = url.pop()
    dest = os.path.splitext(dest)[0] + '.parquet'
    url.append(dest)
    url = '/'.join(url)
    df.write_parquet(url)
    return url

to_df = compose(to_parquet, pl.read_parquet)