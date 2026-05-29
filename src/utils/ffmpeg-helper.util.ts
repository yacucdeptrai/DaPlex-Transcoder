import { Progress } from '../common/entities';

export class FFmpegHelper {
  parseProgress(data: string) {
    const tLines = data.split('\n');
    const progress = new Progress();
    for (var i = 0; i < tLines.length; i++) {
      const key = tLines[i].split('=');
      switch (key[0]) {
        case 'frame':
          progress.frame = Number(key[1]);
          break;
        case 'fps':
          progress.fps = Number(key[1]);
          break;
        case 'bitrate':
          progress.bitrate = key[1];
          break;
        case 'total_size':
          progress.totalSize = Number(key[1]);
          break;
        case 'out_time_us':
          progress.outTimeUs = Number(key[1]);
          break;
        case 'out_time_ms':
          progress.outTimeMs = Number(key[1]);
          break;
        case 'out_time':
          progress.outTime = key[1];
          break;
        case 'dup_frames':
          progress.dupFrames = Number(key[1]);
          break;
        case 'drop_frames':
          progress.dropFrames = Number(key[1]);
          break;
        case 'speed':
          progress.speed = key[1].trim();
          break;
        case 'progress':
          progress.progress = key[1];
          break;
      }
    }
    return progress;
  }

  progressPercent(current: number, videoDuration: number) {
    return videoDuration ? Math.trunc(current / videoDuration * 100) : 0;
  }

  getProgressMessage(progress: Progress, percent: number) {
    return `Encoding: ${percent}% - frame: ${progress.frame || 'N/A'} - fps: ${progress.fps || 'N/A'} - bitrate: ${progress.bitrate} - time: ${progress.outTime} - speed: ${progress.speed}`;
  }

  findH264ProfileLevel(srcWidth: number, srcHeight: number, targetHeight: number, fps: number) {
    const targetWidth = targetHeight * srcWidth / srcHeight;
    const targetFrameSize = targetWidth * targetHeight;
    // 4K 2160p
    if (targetFrameSize >= (3840 * 2160)) {
      if (targetFrameSize <= (4096 * 2160)) {
        if (fps <= 28)
          return '5.1';
        if (fps <= 60)
          return '5.2';
        return null;
      }
      if (targetFrameSize <= (4096 * 2304)) {
        if (fps <= 26)
          return '5.1';
        if (fps <= 56)
          return '5.2';
        return null;
      }
    }
    // 2K 1440p
    if (targetFrameSize >= (2560 * 1440)) {
      if (fps <= 30)
        return '5';
      if (fps <= 60)
        return '5.1';
      return null;
    }
    // FHD 1080p
    if (targetFrameSize >= (1920 * 1080)) {
      if (targetFrameSize <= (2048 * 1088)) {
        if (fps <= 30)
          return '4.1';
        if (fps <= 60)
          return '4.2';
        return null;
      }
      if (targetFrameSize <= (2560 * 1439)) {
        if (fps <= 30)
          return '5';
        if (fps <= 60)
          return '5.1';
        return null;
      }
    }
    // HD 720p
    if (targetFrameSize >= (1280 * 720)) {
      if (targetFrameSize === 1280 * 720) {
        if (fps <= 30)
          return '3.1';
        if (fps <= 60)
          return '3.2';
      }
      if (targetFrameSize <= (1280 * 1024)) {
        if (fps <= 30)
          return '3.2';
        if (fps <= 60)
          return '4.2';
        return null;
      }
      if (targetFrameSize <= (1920 * 1079)) {
        if (fps <= 30)
          return '5';
        if (fps <= 60)
          return '5.1';
        return null;
      }
    }
    // SD 480p
    if (targetFrameSize >= (854 * 480)) {
      if (targetFrameSize <= 720 * 576) {
        return '3.1';
      }
      if (targetFrameSize <= 1280 * 719) {
        return '3.2';
      }
    }
    return null;
  }
}

export const ffmpegHelper = new FFmpegHelper();