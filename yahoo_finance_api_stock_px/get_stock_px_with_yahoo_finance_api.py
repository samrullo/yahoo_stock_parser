import os
import pandas as pd
import numpy as np
from yahoo_finance import Share
import datetime
import logging

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

start = datetime.date(2020, 1, 1)
end = datetime.date(2020, 6, 12)

klbf=Share("YHOO")
print(klbf.get_open())