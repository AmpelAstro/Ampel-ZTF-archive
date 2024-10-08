
# renovate: datasource=conda depName=conda-forge/python
ARG PYTHON_VERSION=3.12.6

FROM python:$PYTHON_VERSION-slim AS base

WORKDIR /app

FROM base AS builder

# renovate: datasource=pypi depName=poetry versioning=pep440
ARG POETRY_VERSION=1.8.3

ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN pip install "poetry==$POETRY_VERSION"
RUN python -m venv /venv

COPY pyproject.toml poetry.lock ./
RUN VIRTUAL_ENV=/venv poetry install --no-root --all-extras

COPY ampel ampel
COPY README.md README.md
RUN poetry build && /venv/bin/pip install dist/*.whl

FROM base AS final

# create cache dirs for astropy and friends
RUN mkdir -p --mode a=rwx /var/cache/astropy
ENV XDG_CACHE_HOME=/var/cache XDG_CONFIG_HOME=/var/cache

COPY --from=builder /venv /venv
CMD ["/venv/bin/uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]

EXPOSE 80
