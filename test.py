from sqlalchemy import create_engine
import psycopg2
import pandas as pd
from linkedin_scraper import scrape_linkedin

engine = create_engine('postgresql+psycopg2://postgres:#B3rniejr01\
@localhost/jobs_db')


scrape_linkedin()
