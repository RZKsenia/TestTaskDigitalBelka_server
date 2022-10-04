FROM python

MAINTAINER Kseniya Epanchina 'rzksenia@yandex.ru'

WORKDIR /

COPY . /

RUN pip install cryptography
RUN pip install -r requirements.txt
RUN pip install waitress

EXPOSE 8080

ENV PYTHONPATH "${PYTHONPATH}:/"

CMD ["python", "server.py"]
CMD ["waitress-serve", "--host", "172.17.0.3", "server:app"]

