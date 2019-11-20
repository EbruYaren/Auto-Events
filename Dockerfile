FROM python:3.7-slim

WORKDIR /usr/src/app
ADD requirements.txt .

RUN pip3 install pandas==0.25.2 \
                 pymongo[tls,srv]==3.9 \
                 psycopg2-binary==2.8.4 \
                 boto3==1.10.2 \
                 requests==2.22.0 \
                 SQLAlchemy==1.3.10 && \
    pip3 install -r requirements.txt

ADD . .
CMD tail -f /dev/null