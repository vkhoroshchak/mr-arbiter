FROM tiangolo/uvicorn-gunicorn:python3.8
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
EXPOSE 5000
COPY . /app
CMD ["uvicorn", "main:app","--host=0.0.0.0", "--port=5000"]


