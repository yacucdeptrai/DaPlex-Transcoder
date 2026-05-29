export const PORT = 3001;
export const ADDRESS = '0.0.0.0';
export const SNOWFLAKE_EPOCH = 1609459200000;
export const SNOWFLAKE_MACHINE_ID = 2;
export const ENCODING_QUALITY = [2160, 1440, 1080, 720, 480, 360];
export const NEXT_GEN_ENCODING_QUALITY = [2160, 1440];
export const AUDIO_PARAMS = ['-c:a', 'libfdk_aac', '-vbr', '5', '-ac', '2'];
export const AUDIO_SPEED_PARAMS = ['-c:a', 'libopus', '-vbr', 'on', '-ac', '2'];
export const AUDIO_SURROUND_PARAMS = ['-c:a', 'libfdk_aac', '-vbr', '5'];
export const AUDIO_SURROUND_OPUS_PARAMS = ['-c:a', 'libopus', '-vbr', 'on'];
export const VIDEO_H264_PARAMS = ['-c:v', 'libx264', '-preset', 'slow', '-crf', '18'];
export const VIDEO_H265_PARAMS = ['-c:v', 'libx265', '-preset', 'slow', '-crf', '18'];
export const VIDEO_VP9_PARAMS = ['-c:v', 'libvpx-vp9', '-crf', '24', '-b:v', '0'];
export const VIDEO_AV1_PARAMS = ['-c:v', 'libsvtav1', '-crf', '20', '-preset', '4'];
export const BYPASS_PRODUCER_CHECK_FILE = 'data/bypass-producer-check';
export const PRODUCER_DOMAINS_FILE = 'data/producer-domains.txt';
export const SPLIT_SEGMENT_FOLDER = 'segments';
export const CONCAT_SEGMENT_FILE = 'concat.txt';
export const THUMBNAIL_FOLDER = 'thumbnails';

// FFmpeg network resilience: retry once and reconnect on transient HTTP errors.
export const FFMPEG_RECONNECT_ARGS = ['-reconnect', '1', '-reconnect_on_http_error', '400,401,403,408,409,429,5xx'];

// HDR -> SDR tonemap filter chain (BT.709). Callers append an output pixel format as needed.
export const HDR_TONEMAP_FILTER =
  'zscale=t=linear:npl=100,format=gbrpf32le,tonemap=tonemap=mobius:desat=0,zscale=p=bt709:t=bt709:m=bt709:r=tv:d=error_diffusion';

// 1 source file (0 for linked source), this many audio renditions, plus the video files.
export const EXPECTED_AUDIO_STREAMS = 3;
// Opus stereo target bitrate (Kbps).
export const OPUS_STEREO_BITRATE = 128;
// Opus surround per-channel bitrate (Kbps).
export const OPUS_SURROUND_BITRATE_PER_CHANNEL = 64;
// 8 channels (7.1) is the limit for both aac and opus.
export const MAX_AUDIO_CHANNELS = 8;
// Channel layouts that warrant a surround Opus encode: 5 (4.1), 6 (5.1), 7 (6.1), 8 (7.1).
export const SURROUND_CHANNEL_COUNTS = [5, 6, 7, 8];