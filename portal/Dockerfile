# The portal, for your Kubernetes cluster (deploy/de.py portal_image). Build context: the repo root.
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY fairdemo ./fairdemo
COPY portal ./portal
RUN pip install --no-cache-dir ".[portal]" "vastdb[adbc]>=2.1" \
    && rm -rf /root/.cache
RUN chown -R 1000:1000 /app
ENV HOME=/app
USER 1000
EXPOSE 8080
CMD ["uvicorn", "portal.app:app", "--host", "0.0.0.0", "--port", "8080"]
