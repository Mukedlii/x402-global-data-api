FROM apify/actor-python:3.11

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install -r requirements.txt --no-cache-dir

COPY . ./

CMD ["python", "src/main.py"]
