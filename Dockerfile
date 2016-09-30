FROM alpine:3.4

RUN apk add --no-cache python3 bash git && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --upgrade pip setuptools && \
    rm -r /root/.cache

RUN git clone https://github.com/Cosive/etl2.git /app

RUN cd /app && git checkout devel

COPY entrypoint.sh /app/entrypoint.sh

COPY wheels /app/wheels

RUN chmod 755 /app/entrypoint.sh

RUN pip3 -vvv install --use-wheel --no-index --find-links=/app/wheels/ -r /app/requirements.txt && rm -rf /app/wheels

ENTRYPOINT /app/entrypoint.sh
