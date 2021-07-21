FROM continuumio/miniconda3:4.9.2 AS build-env

# Create a conda environment with python + pip
COPY conda-linux-64.lock .
RUN conda create --prefix /venv --file conda-linux-64.lock

# Build a wheel and requirements.txt so we can install the
# locked deps without needing poetry in the target env
RUN pip install poetry
COPY pyproject.toml poetry.lock .
RUN poetry export -f requirements.txt -E server -E binary --output requirements.txt
COPY README.md /
COPY ampel /ampel
RUN poetry build -f wheel -n

# Make RUN commands use the new environment:
SHELL ["conda", "run", "--prefix", "/venv", "/bin/bash", "-c"]

# Install deps and wheel
RUN pip install -r requirements.txt && \
  pip install --no-deps dist/*.whl

FROM gcr.io/distroless/base-debian10

# Set up venv
ENV PATH="/venv/bin:$PATH"

# Copy the source code into the distroless image
COPY --from=build-env /venv /venv

CMD ["uvicorn", "ampel.ztf.archive.server.app:app"]