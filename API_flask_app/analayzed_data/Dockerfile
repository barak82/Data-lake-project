FROM python:slim
COPY . .
RUN python -m venv venv
RUN venv/bin/pip install -r requirements.txt
RUN venv/bin/pip install gunicorn
RUN chmod +x ./boot.sh
ENV FLASK_APP API_client.py
RUN venv/bin/pip install gunicorn pymysql cryptography
EXPOSE 6060
ENTRYPOINT ["./boot.sh"]