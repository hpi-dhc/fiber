# FIBER stage
FROM python:3.6 as fiber

COPY . fiber

WORKDIR /fiber/

RUN pip install -r requirements.txt \
    && pip install -e .

# Jupyter notebook stage
FROM fiber

RUN pip install jupyterlab

EXPOSE 8888

CMD ["jupyter", "notebook", "--ip", "0.0.0.0", "--no-browser", "--allow-root"]