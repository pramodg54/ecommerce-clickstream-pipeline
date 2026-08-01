FROM apache/spark:3.5.1

USER root

RUN pip3 install --no-cache-dir \
    delta-spark==3.2.0 \
    google-cloud-bigquery \
    google-auth \
    pyarrow \
    pandas \
 && ln -sf /usr/bin/python3 /usr/bin/python

USER spark

WORKDIR /opt/project