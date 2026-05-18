FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ml_nlp_analytics_job.py .

CMD ["python", "ml_nlp_analytics_job.py"]
