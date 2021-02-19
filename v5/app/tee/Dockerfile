FROM sconecuratedimages/public-apps:python-3.7.3-alpine3.10-scone3.0

### install some python3 dependencies
RUN apk --no-cache --update-cache add gcc libc-dev libffi-dev g++
RUN SCONE_MODE=sim pip3 install --no-cache-dir google-cloud-bigquery
RUN \
    apk del libffi-dev g++ && \
    rm -rf /var/cache/apk/* && \
    rm -rf /var/lib/apk/* && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /etc/apk/cache/*

### copy the code inside the image
COPY ./src /app

### protect file system with Scone
COPY ./tee/protect-fs.sh ./tee/Dockerfile /build/
RUN sh /build/protect-fs.sh /app

ENTRYPOINT ["python", "/app/app.py"]
