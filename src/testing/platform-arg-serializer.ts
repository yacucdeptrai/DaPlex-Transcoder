/**
 * Jest snapshot serializer that normalizes the two OS-dependent ffmpeg arg tokens
 * to their canonical (Windows-recorded) form, so the committed snapshots match on
 * both Windows and Linux/WSL:
 *   - the null sink: '/dev/null' (posix) -> 'NUL'
 *   - the segment-init name: '\$Init=\$' (posix shell-escaped) -> '$Init=$'
 *
 * Only strings containing exactly these tokens are touched; every other snapshot
 * value passes through unchanged. This keeps the encoding-args / two-pass snapshot
 * suites platform-agnostic without weakening any assertion (the explicit
 * platform-aware checks in the specs still read the raw, un-normalized args).
 */
const NULL_SINK_POSIX = '/dev/null';
const NULL_SINK_CANONICAL = 'NUL';
const SEGMENT_INIT_POSIX = '\\$Init=\\$';
const SEGMENT_INIT_CANONICAL = '$Init=$';

function normalize(value: string): string {
  return value.split(NULL_SINK_POSIX).join(NULL_SINK_CANONICAL).split(SEGMENT_INIT_POSIX).join(SEGMENT_INIT_CANONICAL);
}

module.exports = {
  test(value: unknown): boolean {
    return typeof value === 'string' && (value.includes(NULL_SINK_POSIX) || value.includes(SEGMENT_INIT_POSIX));
  },
  serialize(value: string): string {
    return `"${normalize(value)}"`;
  }
};
