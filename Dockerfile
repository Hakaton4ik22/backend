# FROM python:3.9
FROM python:3.9.7-slim

EXPOSE 8000
WORKDIR /app

COPY . /app
RUN pip install -e .

CMD ["uvicorn", "app.main:app", "--host", "192.0.0.1", "--port", "8000"]


#FROM python:3.9.7-slim

#ENV PYTHONUNBUFFERED 1

#EXPOSE 8000
#WORKDIR /app

#COPY . /app
#RUN pip install -e .