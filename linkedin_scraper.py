import logging
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, RemoteFilters
from datetime import date
import pandas as pd
import sqlalchemy
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
    # Remember - storing secrets in plaintext is potentially unsafe. Consider using
    # something like https://cloud.google.com/secret-manager/docs/overview to help keep
    # secrets secret.
    db_user = os.environ["POSTGRES-USER"]
    db_pass = os.environ["POSTGRES-PASS"]
    db_name = os.environ["POSTGRES-NAME"]
    db_socket_dir = os.environ.get("DB_SOCKET_DIR", "/cloudsql")
    instance_connection_name = os.environ["POSTGRES-CONNECTION-NAME"]

    pool = sqlalchemy.create_engine(

        # Equivalent URL:
        # postgresql+pg8000://<db_user>:<db_pass>@/<db_name>
        #                         ?unix_sock=<socket_path>/<cloud_sql_instance_name>/.s.PGSQL.5432
        # Note: Some drivers require the `unix_sock` query parameter to use a different key.
        # For example, 'psycopg2' uses the path set to `host` in order to connect successfully.
        sqlalchemy.engine.url.URL.create(
            drivername="postgresql+pg8000",
            username=db_user,  # e.g. "my-database-user"
            password=db_pass,  # e.g. "my-database-password"
            database=db_name,  # e.g. "my-database-name"
            query={
                "unix_sock": "{}/{}/.s.PGSQL.5432".format(
                    db_socket_dir,  # e.g. "/cloudsql"
                    instance_connection_name)  # i.e "<PROJECT-NAME>:<INSTANCE-REGION>:<INSTANCE-NAME>"
            }
        ),
    )

    # run scraper - appends to jobs
    scraper.run(queries)
    df = pd.DataFrame(jobs, columns=['title', 'location', 'link', 'company',
                                     'date', 'description'])
    df['platform'] = 'LinkedIn'

    # engine = create_engine(
    #     'postgresql+psycopg2://postgres:#B3rniejr01@host.docker.internal/jobs_db')

    with pool.connect() as con:
        df.to_sql('jobs', con=con, if_exists='append', index=False)

    return 'Success!'
