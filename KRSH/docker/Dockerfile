# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    EXECUTION_MODE=PAPER

WORKDIR /opt/krsh

COPY app/requirements.txt /opt/krsh/requirements.txt
RUN pip install --upgrade pip && pip install -r /opt/krsh/requirements.txt

COPY app/src /opt/krsh/src
COPY app/data /opt/krsh/data
COPY app/.streamlit /opt/krsh/.streamlit

EXPOSE 8501

CMD ["streamlit", "run", "src/Trading.py", "--server.address=0.0.0.0", "--server.port=8501"]
