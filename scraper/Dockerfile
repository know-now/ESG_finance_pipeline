FROM python:3.10-slim
WORKDIR /app
COPY scraper/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY scraper/esg_scraper.py .
CMD ["python", "-u", "esg_scraper.py"]