FROM python:latest

WORKDIR /app

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY utils.py .
COPY pipeline.py .
COPY data/institutes.csv .

CMD ["python3", "pipeline.py"]