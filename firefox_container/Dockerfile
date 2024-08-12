FROM python:3.9-slim

ENV TZ="America/Sao_Paulo"

RUN install -d -m 0755 /etc/apt/keyrings
RUN apt-get update && apt-get install wget -y
RUN wget -q https://packages.mozilla.org/apt/repo-signing-key.gpg -O- | tee /etc/apt/keyrings/packages.mozilla.org.asc > /dev/null

RUN echo "deb [signed-by=/etc/apt/keyrings/packages.mozilla.org.asc] https://packages.mozilla.org/apt mozilla main" | tee -a /etc/apt/sources.list.d/mozilla.list > /dev/null
RUN apt-get update && apt-get install firefox -y

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY ./$APP_HOME .

CMD ["python", "-u", "main.py"]