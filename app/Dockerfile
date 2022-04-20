FROM python:latest
WORKDIR /code
ADD requirements.txt requirements.txt
FROM python:latest
COPY /app/pipeline/
COPY /app/static/
COPY /app/templates/
COPY app.py app.py
WORKDIR /app/pipeline
CMD ["python", "retrieve_historical_data.py"]
CMD ["python", "daily_pull.py"]