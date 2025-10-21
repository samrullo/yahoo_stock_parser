# -*- coding: utf-8 -*-
"""
Created on Mon Jul 16 21:30:17 2018

@author: amrul
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
import sys


def add_ret_cols(df, col):
    """
    Add returns columns
    :param df:
    :param col:
    :return:
    """
    df["dly_ret"] = df[col].pct_change()
    df["dly_ret_bps"] = df["dly_ret"] * 10000
    df["comp_ret"] = np.cumprod(1 + df["dly_ret"])
    df["up"] = df["dly_ret_bps"].map(lambda x: x > 0)
    return df.copy()


def setup_custom_logger(name, log_filename, log_level=logging.INFO):
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler = logging.FileHandler(log_filename, mode="w")
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.addHandler(handler)
    logger.addHandler(screen_handler)
    return logger
