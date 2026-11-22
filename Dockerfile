FROM apache/spark:3.5.3
USER root
RUN pip install --no-cache-dir kafka-python==2.0.2 pyarrow==17.0.0 psycopg2-binary==2.9.10 psutil==6.1.1 matplotlib==3.7.5 reportlab==3.6.13
WORKDIR /work
ENV PYTHONPATH=/work/src
