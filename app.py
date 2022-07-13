from flask import Flask
from linkedin_scraper import scrape_linkedin

app = Flask(__name__)

app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"


@app.route('/')
def home():
    return "Home Page 1"


@app.route('/linkedin')
def linkedin():

    # Scrape LinkedIn and send to PostgreSQL DB
    result = scrape_linkedin()

    return result.to_string()


@app.route('/linkedin-newtable')
def linkedin_newtable():

    # Scrape LinkedIn and send to PostgreSQL DB
    result = scrape_linkedin(if_exists='replace')

    return result.to_string()


@ app.route('/indeed')
def indeed():

    return 'indeed!'


if __name__ == '__main__':
    app.run(debug=False)
