import os
import sys
import time
from dotenv import load_dotenv
load_dotenv(override = True)
import pandas as pd
from sqlalchemy import create_engine


print("===Extract, Transform, Load (ETL) Started ===")

try:
    # Block Extract (E): creation of raw test data about finance