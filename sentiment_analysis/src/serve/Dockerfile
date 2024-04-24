FROM python:3.9

# Set the working directory in the container to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY serve/predict.py /app/

RUN pip install flask google-cloud-storage werkzeug python-dotenv pandas joblib tensorflow mlflow

ENV AIP_STORAGE_URI=gs://mlops_pro/model
ENV AIP_HEALTH_ROUTE=/ping
ENV AIP_PREDICT_ROUTE=/predict
ENV BUCKET_NAME=mlops_pro
ENV PROJECT_ID=mlops-421203
ENV AIP_HTTP_PORT=8080


ENTRYPOINT ["python", "predict.py"]

