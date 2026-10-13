# syntax=docker/dockerfile:1
FROM rust:1.85.1-bookworm AS rust-build
WORKDIR /build/rust-protocol
COPY rust-protocol/Cargo.toml rust-protocol/Cargo.lock ./
COPY rust-protocol/src ./src
RUN cargo build --locked --release --bin mas --bin mas-gateway

FROM python:3.12-slim-bookworm AS python-build
WORKDIR /build/host
COPY host/pyproject.toml host/requirements-openai.txt ./
COPY host/mas_host ./mas_host
RUN python -m pip wheel --no-cache-dir --wheel-dir /wheels '.[openai]'

FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MAS_BIN=/usr/local/bin/mas \
    MAS_GATEWAY_BIN=/usr/local/bin/mas-gateway \
    MAS_MODE=mock \
    MAS_STATE_DIR=/state \
    MAS_TOPOLOGY=/app/examples/compose-mesh.json
RUN groupadd --gid 10001 mas \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /state mas \
    && mkdir -p /state /app/examples \
    && chown mas:mas /state \
    && chmod 0700 /state
COPY --from=rust-build /build/rust-protocol/target/release/mas /usr/local/bin/mas
COPY --from=rust-build /build/rust-protocol/target/release/mas-gateway /usr/local/bin/mas-gateway
COPY --from=python-build /wheels /wheels
RUN python -m pip install --no-cache-dir --no-index --find-links=/wheels 'mas-host[openai]==0.1.0' \
    && rm -rf /wheels
COPY examples/compose-mesh.json /app/examples/compose-mesh.json
COPY LICENSE THIRD_PARTY_LICENSE.txt /usr/share/doc/mas/
WORKDIR /app
USER 10001:10001
ENTRYPOINT ["python", "-m", "mas_host"]
