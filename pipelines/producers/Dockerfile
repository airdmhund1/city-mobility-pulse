FROM python:3.11-slim
WORKDIR /app
COPY pipelines/producers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipelines/producers/ .
CMD ["python","producer_bike.py"]
