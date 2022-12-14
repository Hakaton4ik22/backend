FROM python:3.9

EXPOSE 8000
WORKDIR /app

COPY . /app
RUN pip install -e .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

