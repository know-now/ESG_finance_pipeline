FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY processor/esg_consumer_test.py .
CMD ["python", "-u", "esg_consumer_test.py"]
