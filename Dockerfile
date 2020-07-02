FROM python:3.7

RUN pip install --upgrade pip
RUN pip install supervisor
RUN apt-get update && \
     apt-get install -y libpoppler-private-dev libpoppler-cpp-dev && pip install cython && pip install git+https://github.com/izderadicka/pdfparser



COPY . /app
WORKDIR /app

COPY configs/supervisord.conf  /etc/supervisor/conf.d/supervisord.conf

RUN pip install -r requirements.txt --no-cache-dir

CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]

