// Minimal fake env for the bootstrap smoke. The Transcoder has no env-validation
// DTO; only the connection factories read env (DATABASE_URL, REDIS_QUEUE_URL),
// and those connections are overridden/mocked in the smoke, so nothing here
// opens a socket. Import for side effect before AppModule is loaded.
const FAKE_ENV: Record<string, string> = {
  DATABASE_URL: 'mongodb://127.0.0.1:27017/daplex_transcoder_test',
  REDIS_QUEUE_URL: 'redis://127.0.0.1:6379'
};

for (const [key, value] of Object.entries(FAKE_ENV)) {
  if (process.env[key] === undefined) process.env[key] = value;
}
