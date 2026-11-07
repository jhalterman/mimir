# SPDX-License-Identifier: AGPL-3.0-only

ARG        BASEIMG=gcr.io/distroless/static-debian13@sha256:448810357cc8e67b7208988abdba0f5ec3d3aebee0a5b13eb624979792cffc0f

FROM       ${BASEIMG}
COPY       fake-ingester /fake-ingester
ENTRYPOINT ["/fake-ingester"]

ARG revision
LABEL org.opencontainers.image.title="fake-ingester" \
      org.opencontainers.image.source="https://github.com/grafana/mimir/tree/main/tools/fake-ingester" \
      org.opencontainers.image.revision="${revision}"
