FROM python:3.11-slim

WORKDIR /

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

ADD forex_rates.py .

CMD python -u forex_rates.py