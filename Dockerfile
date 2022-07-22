FROM python:3.8

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ENV FLASK_APP=app.py
ENV FLASK_ENV=development
ENV LI_AT_COOKIE=AQEDATD6TC8CzPjeAAABghYa89wAAAGCOid33FYANhjYHIW_kq7scCAocL93wGSiUwzw4X2zNLWy8TOcLLMjKUU6OPofje2epQIHYPhINPTqQ119s__w-NTPDN6alH6GQusb3Bw-IUJ2NHB3f97TBFtJ

# install system dependencies
RUN apt-get update \
    && apt-get -y install gcc make \
    && rm -rf /var/lib/apt/lists/*s

# install google chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable

# install chromedriver
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

RUN python3 --version
RUN pip3 --version
RUN pip install --no-cache-dir --upgrade pip

WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt
COPY . .


CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]