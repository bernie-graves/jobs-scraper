import logging
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, RemoteFilters
from datetime import date
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os

# Change root logger level (default is WARN)
logging.basicConfig(level=logging.INFO)

jobs = list()


def on_data(data: EventData):
    jobs.append([data.title, data.location, data.apply_link,
                data.company, data.date, data.description])

# Fired once for each page (25 jobs)


def on_metrics(metrics: EventMetrics):
    print('[ON_METRICS]', str(metrics))


def on_error(error):
    print('[ON_ERROR]', error)


def on_end():
    print('[ON_END]')


scraper = LinkedinScraper(
    # Custom Chrome executable path (e.g. /foo/bar/bin/chromedriver)
    chrome_executable_path=None,
    chrome_options=None,  # Custom Chrome options here
    headless=True,  # Overrides headless mode only if chrome_options is None
    # How many threads will be spawned to run queries concurrently (one Chrome driver for each thread)
    max_workers=1,
    # Slow down the scraper to avoid 'Too many requests 429' errors (in seconds)
    slow_mo=0.5,
    page_load_timeout=20  # Page load timeout (in seconds)
)

# Add event listeners
scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)

queries = [
    Query(
        query='Data Scientist',
        options=QueryOptions(
            locations=['United States'],
            # Try to extract apply link (easy applies are skipped). Default to False.
            apply_link=True,
            limit=5,
            filters=QueryFilters(
                # Filter by companies.
                time=TimeFilters.DAY,
                type=[TypeFilters.INTERNSHIP],
                experience=ExperienceLevelFilters.INTERNSHIP,
            )
        )
    ),
]


def scrape_linkedin():

    # Getting DB connection URL
    db_user = os.environ["DB_USER"]
    db_password = os.environ["DB_PASSWORD"]
    db_name = os.environ["DB_NAME"]
    db_host = os.environ["DB_HOST"]
    db_port = os.environ["DB_PORT"]

    conn_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # connectiong to DB
    engine = create_engine(conn_url)

    # run scraper - appends to jobs
    scraper.run(queries)
    df = pd.DataFrame(jobs, columns=['title', 'location', 'link', 'company',
                                     'date', 'description'])
    df['platform'] = 'LinkedIn'

    # post to DB
    with engine.connect() as con:
        df.to_sql('jobs', con=con, if_exists='append', index=False)

    return 'Success!'
