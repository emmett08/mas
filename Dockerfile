FROM python:3.14-alpine@sha256:f6a589d43c42b9e7f7dc67a12d37132491f362859a5d750607710cc56da3bc72 AS alpine-packages
ARG TARGETARCH
COPY docker/alpine-packages.json /build/alpine-packages.json
COPY scripts/release-apk.py /build/release-apk.py
RUN python /build/release-apk.py --lock /build/alpine-packages.json --arch "$TARGETARCH" --out /packages

FROM rust:1.85.1-alpine@sha256:4333721398de61f53ccbe53b0b855bcc4bb49e55828e8f652d7a8ac33dd0c118 AS rust-build
COPY --from=alpine-packages /packages/build /packages
RUN --network=none apk add --no-network /packages/*.apk && rm -rf /packages
ENV RUSTFLAGS="-C target-feature=-crt-static" \
    LIBSQLITE3_SYS_USE_PKG_CONFIG=1
WORKDIR /build/rust-protocol
COPY rust-protocol/Cargo.toml rust-protocol/Cargo.lock ./
COPY rust-protocol/src ./src
RUN cargo build --locked --release --bin mas --bin mas-gateway

FROM python:3.14-alpine@sha256:f6a589d43c42b9e7f7dc67a12d37132491f362859a5d750607710cc56da3bc72 AS python-build
WORKDIR /build/host
COPY host/pyproject.toml host/requirements-openai.txt host/requirements-build.txt ./
RUN python -m pip install --no-cache-dir --require-hashes -r requirements-build.txt \
    && python -m pip download --no-cache-dir --require-hashes --dest /wheels -r requirements-openai.txt
COPY host/mas_host ./mas_host
RUN --network=none python -m pip wheel --no-cache-dir --no-index --no-deps --no-build-isolation --wheel-dir /wheels . \
    && python -c 'from pathlib import Path; import hashlib; p=next(Path("/wheels").glob("mas_host-*.whl")); Path("/wheels/host-wheel.txt").write_text(str(p)+" --hash=sha256:"+hashlib.sha256(p.read_bytes()).hexdigest()+"\n")'

FROM python:3.14-alpine@sha256:f6a589d43c42b9e7f7dc67a12d37132491f362859a5d750607710cc56da3bc72
LABEL org.opencontainers.image.source="https://github.com/emmett08/mas"
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MAS_BIN=/usr/local/bin/mas \
    MAS_GATEWAY_BIN=/usr/local/bin/mas-gateway \
    MAS_MODE=mock \
    MAS_STATE_DIR=/state \
    MAS_TOPOLOGY=/app/examples/host/mesh.json
COPY --from=alpine-packages /packages/runtime /packages
RUN --network=none apk add --no-network /packages/*.apk && rm -rf /packages
RUN addgroup -g 10001 mas \
    && adduser -D -H -u 10001 -G mas -h /state mas \
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
