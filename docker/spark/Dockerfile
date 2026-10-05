FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

USER root

RUN pip install --no-cache-dir \
    pyspark==4.1.3 \
    jupyterlab \
    pandas \
    requests \
    minio \
    python-dotenv\
    streamlit

WORKDIR /workspace

EXPOSE 8888
EXPOSE 4040

CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--allow-root", \
     "--NotebookApp.token="]