FROM python:3.9

WORKDIR /

ENV AIP_STORAGE_URI=gs://mlops_pro/model
ENV BUCKET_NAME=mlops_pro

COPY trainer/requirements.txt .
COPY trainer /trainer

RUN pip install --no-cache-dir -r trainer/requirements.txt


ENTRYPOINT ["python","-m","trainer.train"]



