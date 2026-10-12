FROM python:3.10-slim
WORKDIR /app
RUN apt-get update && apt-get install -y gcc
RUN pip install --upgrade pip

RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu --extra-index-url https://pypi.org/simple
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY processor/esg_consumer.py .
CMD ["python", "-u", "esg_consumer.py"]
