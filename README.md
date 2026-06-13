# DaPlex Transcoder

NestJS (Fastify) worker for the DaPlex platform. It consumes BullMQ jobs produced by [`../DaPlex-API`](../DaPlex-API) and runs the media pipeline — probing sources, transcoding to multiple codecs/qualities, packaging HLS/DASH, and syncing outputs to remote storage via rclone. It shares MongoDB and the BullMQ Redis with the API.

## Stack

- **NestJS 10** (`@nestjs/platform-fastify`)
- **BullMQ** consumer + ioredis
- **Mongoose 7** — persistent connection, same database as the API
- **Media tooling** — ffprobe (`ffprobe-client`), `m3u8-parser` (HLS), `fast-xml-parser` (DASH), `check-disk-space`, `chokidar`
- **Logging** — Winston (`nest-winston`, daily-rotate file)
- **Build** — SWC; **Tests** — Jest + ts-jest

## Prerequisites

- **Node.js 20+** (workspace runs Node 24)
- **npm 10+**
- A running **Redis** — the same instance the API uses for queues (see [`../Redis`](../Redis))
- A reachable **MongoDB** — the same database the API uses
- **External binaries on the host**, located via env paths:
  - **FFmpeg** → `FFMPEG_DIR`
  - **MediaInfo** → `MEDIAINFO_DIR`
  - **MP4Box / GPAC** → `MP4BOX_DIR`
  - **rclone** → `RCLONE_DIR`, `RCLONE_CONFIG_FILE` (remote storage)

## Install

```bash
npm install
```

## Configure

This service does **not** ship a `.env.example`; create a `.env`. Defaults: `PORT=3001`, `ADDRESS=0.0.0.0` (see `src/config.ts`).

| Group | Variables |
|-------|-----------|
| Server | `PORT` (default `3001`), `ADDRESS` (default `0.0.0.0`), `NODE_ENV` |
| Data | `DATABASE_URL` (same Mongo as the API), `REDIS_QUEUE_URL` (same BullMQ Redis as the API) |
| Crypto | `CRYPTO_SECRET_KEY` — must match the API; used to decrypt source-link tokens |
| Tool paths | `FFMPEG_DIR`, `MEDIAINFO_DIR`, `MP4BOX_DIR`, `RCLONE_DIR`, `RCLONE_CONFIG_FILE`, `TRANSCODE_DIR` |
| Encoding | `VIDEO_CODEC`, `VIDEO_H264_PARAMS`, `VIDEO_H265_PARAMS`, `VIDEO_VP9_PARAMS`, `VIDEO_AV1_PARAMS`, `AUDIO_PARAMS`, `AUDIO_SURROUND_PARAMS`, … |
| Pipeline | `USE_URL_INPUT`, `SPLIT_ENCODING`, `SPLIT_SEGMENT_DURATION`, `PRIMARY_TRANSCODER_URL`, `DAPLEX_API_DOMAINS` |

`DATABASE_URL` and `REDIS_QUEUE_URL` must point at the **same** MongoDB/Redis the API uses, or jobs won't be shared between them.

## Run

```bash
# dev (watch)
npm run start:dev

# dev with debugger attached
npm run start:debug

# production
npm run build        # nest build -> dist/
npm run start:prod   # node dist/main
```

Listens on `ADDRESS:PORT` (default `0.0.0.0:3001`). Logs are written to `logs/` (Winston daily-rotate). Shutdown hooks are enabled for clean queue/connection teardown.

## Test

No bare `test` script — run Jest directly:

```bash
npx jest             # one-shot unit tests (*.spec.ts)
npm run test:cov     # with coverage
npm run test:watch   # watch mode
npm run test:e2e     # e2e
```

**Testing note:** FFmpeg-argument snapshots are normalized by a platform serializer (`src/testing/platform-arg-serializer.ts`) so Windows-recorded snapshots also pass on Linux/WSL. Do **not** run `jest -u` on Linux/WSL — it would rewrite those snapshots against the wrong platform. Refresh snapshots only when the argument logic actually changes.

## Lint & format

```bash
npm run lint     # eslint --fix
npm run format   # prettier
```

## Relationship to other services

- Driven by [`../DaPlex-API`](../DaPlex-API) through BullMQ (`REDIS_QUEUE_URL`).
- Reads/writes the same MongoDB as the API (`DATABASE_URL`).
- Requires [`../Redis`](../Redis) (or any Redis 6+) running first.
