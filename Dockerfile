FROM rust:1.85.1-bookworm@sha256:e51d0265072d2d9d5d320f6a44dde6b9ef13653b035098febd68cce8fa7c0bc4 AS rust-build
WORKDIR /build/rust-protocol
COPY rust-protocol/Cargo.toml rust-protocol/Cargo.lock ./
COPY rust-protocol/src ./src
RUN cargo build --locked --release --bin mas --bin mas-gateway

FROM python:3.12-slim-trixie@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1 AS python-build
WORKDIR /build/host
COPY host/pyproject.toml host/requirements-openai.txt host/requirements-build.txt ./
RUN python -m pip install --no-cache-dir --require-hashes -r requirements-build.txt \
    && python -m pip download --no-cache-dir --require-hashes --dest /wheels -r requirements-openai.txt
COPY host/mas_host ./mas_host
RUN --network=none python -m pip wheel --no-cache-dir --no-index --no-deps --no-build-isolation --wheel-dir /wheels . \
    && python -c 'from pathlib import Path; import hashlib; p=next(Path("/wheels").glob("mas_host-*.whl")); Path("/wheels/host-wheel.txt").write_text(str(p)+" --hash=sha256:"+hashlib.sha256(p.read_bytes()).hexdigest()+"\n")'

FROM python:3.12-slim-trixie@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1
LABEL org.opencontainers.image.source="https://github.com/emmett08/mas"
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MAS_BIN=/usr/local/bin/mas \
    MAS_GATEWAY_BIN=/usr/local/bin/mas-gateway \
    MAS_MODE=mock \
    MAS_STATE_DIR=/state \
    MAS_TOPOLOGY=/app/examples/host/mesh.json
RUN groupadd --gid 10001 mas \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /state mas \
    && mkdir -p /state /app/examples \
    && chown mas:mas /state \
    && chmod 0700 /state
COPY --from=rust-build /build/rust-protocol/target/release/mas /usr/local/bin/mas
COPY --from=rust-build /build/rust-protocol/target/release/mas-gateway /usr/local/bin/mas-gateway
COPY --from=python-build /wheels /wheels
COPY host/requirements-openai.txt /wheels/requirements-openai.txt
RUN --network=none python -m pip install --no-cache-dir --no-index --find-links=/wheels --require-hashes -r /wheels/requirements-openai.txt \
    && python -m pip install --no-cache-dir --no-index --no-deps --require-hashes -r /wheels/host-wheel.txt \
    && python -m pip check \
    && rm -rf /wheels
COPY examples/host/mesh.json /app/examples/host/mesh.json
COPY LICENSE THIRD_PARTY_LICENSE.txt /usr/share/doc/mas/
WORKDIR /app
USER 10001:10001
ENTRYPOINT ["python", "-m", "mas_host"]
