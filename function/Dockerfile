# Built for linux/amd64 by ../deploy/de.py image: the build context holds this Dockerfile, main.py, the
# fairdemo package (./fairdemo-src) and the DataEngine SDK and runtime wheels (./wheels).
FROM python:3.12-slim
WORKDIR /workspace
COPY . .
RUN pip install --no-cache-dir ./wheels/vast_dataengine_sdk-*.whl ./wheels/vast_dataengine_runtime-*.whl \
        boto3 "vastdb>=1.3" ./fairdemo-src \
    && rm -rf ./wheels ./fairdemo-src
# Run as non-root (required by VAST DataEngine security policy)
RUN chown -R 1000:1000 /workspace
ENV HOME=/workspace
USER 1000
ENTRYPOINT ["vast-dataengine-runtime", "--app-path", "/workspace", "--handler", "main.py"]
