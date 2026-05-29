import { stdout } from 'process';
import child_process from 'child_process';
import chokidar from 'chokidar';

import { fileHelper } from './file-helper.util';
import { ffmpegHelper } from './ffmpeg-helper.util';
import { createCancelChecker } from './cancel-checker.util';
import { FFMPEG_RECONNECT_ARGS } from '../config';
import { RejectCode } from '../enums';

export interface ConvertOptions {
  ffmpegDir: string;
  rcloneDir: string;
  rcloneConfigFile: string;
  duration: number;
  remote: string;
  remoteParentFolder: string;
  remoteFolder: string;
  keepAudio?: boolean;
  videoOnly?: boolean;
  useURLInput?: boolean;
  jobId: string | number;
  canceledJobIds: (string | number)[];
  logFn?: (message: string) => void;
}

export interface RemuxOptions {
  ffmpegDir: string;
  duration: number;
  keepAudio?: boolean;
  videoOnly?: boolean;
  audioCodec?: string;
  useURLInput?: boolean;
  jobId: string | number;
  canceledJobIds: (string | number)[];
  logFn?: (message: string) => void;
}

export class VideoSourceHelper {
  async fragmentSourceAndUpload(inputFile: string, outputFolder: string, outputFile: string, options: ConvertOptions) {
    await fileHelper.createDir(outputFolder);
    await new Promise<void>((resolve, reject) => {
      let isCancelled = false;
      let rclone: child_process.ChildProcessWithoutNullStreams | null = null;

      const args = [
        '-hide_banner', '-y',
        '-progress', 'pipe:1',
        '-loglevel', 'error'
      ];

      if (options.useURLInput) {
        args.push(
          ...FFMPEG_RECONNECT_ARGS
        );
      }

      args.push(
        '-i', `"${inputFile}"`,
        '-c', 'copy'
      );

      if (!options.keepAudio)
        args.push('-an');
      if (options.videoOnly)
        args.push('-map', '0:v:0');
      args.push(
        '-f', 'fragment',
        '-reset_timestamps', '1',
        `"${outputFolder}/${outputFile}_%03d.mkv"`
      );

      const ffmpeg = child_process.spawn(`"${options.ffmpegDir}/ffmpeg"`, args, { shell: true });
      ffmpeg.stdout.setEncoding('utf8');
      ffmpeg.stdout.on('data', async (data: string) => {
        const progress = ffmpegHelper.parseProgress(data);
        const percent = ffmpegHelper.progressPercent(progress.outTimeMs, options.duration * 1000000);
        stdout.write(`${ffmpegHelper.getProgressMessage(progress, percent)}\r`);
      });

      ffmpeg.stderr.setEncoding('utf8');
      ffmpeg.stderr.on('data', (data) => {
        stdout.write(data);
      });

      const cancelledJobChecker = createCancelChecker(
        () => options.canceledJobIds,
        ids => (options.canceledJobIds = ids),
        options.jobId,
        () => {
          isCancelled = true;
          ffmpeg.stdin.write('q');
          ffmpeg.stdin.end();
          rclone?.kill('SIGINT');
        },
        5000
      );

      const watcher = chokidar.watch(`${outputFolder}/segment_*.mkv`, {
        persistent: true,
        awaitWriteFinish: {
          stabilityThreshold: 5000,
          pollInterval: 100
        }
      });

      const uploadSegment = (filePath: string) => {
        const rcloneArgs = [
          '--config', options.rcloneConfigFile,
          '--low-level-retries', '5',
          'move',
          `"${filePath}"`,
          `"${options.remote}:${options.remoteParentFolder}/${options.remoteFolder}"`
        ];
        return child_process.spawn(`"${options.rcloneDir}/rclone"`, rcloneArgs, { shell: true });
      }

      watcher.on('add', (filePath) => {
        stdout.write(`Segment created: ${filePath}\n`);
        rclone = uploadSegment(filePath);
      });

      ffmpeg.on('exit', (code: number) => {
        stdout.write('\n');
        if (isCancelled) {
          reject(RejectCode.JOB_CANCEL);
        } else if (code !== 0) {
          reject({ code, message: `FFmpeg exited with status code: ${code}` });
        } else {
          stdout.write(`Uploading remaining segments\n`);
          rclone = uploadSegment(`"${outputFolder}"`);
          rclone.on('exit', (code: number) => {
            clearInterval(cancelledJobChecker);
            if (isCancelled) {
              reject(RejectCode.JOB_CANCEL);
            } else if (code !== 0) {
              reject(`Rclone exited with status code: ${code}`);
            } else {
              resolve();
            }
          });
        }
      });
    });
    await fileHelper.deleteFolder(outputFolder);
  }

  async remuxSourceMKV(inputFile: string, outputFile: string, options: RemuxOptions) {
    return new Promise<void>((resolve, reject) => {
      let isCancelled = false;

      const args = [
        '-hide_banner', '-y',
        '-progress', 'pipe:1',
        '-loglevel', 'error'
      ];

      if (options.useURLInput) {
        args.push(
          ...FFMPEG_RECONNECT_ARGS
        );
      }

      args.push(
        '-i', `"${inputFile}"`,
        '-c:v', 'copy'
      );

      if (!options.keepAudio)
        args.push('-an');
      else if (!options.audioCodec)
        args.push('-c:a', 'copy');
      else
        args.push('-c:a', options.audioCodec);
      if (options.videoOnly)
        args.push('-map', '0:v:0');
      args.push(`"${outputFile}"`);

      options.logFn && options.logFn('ffmpeg ' + args.join(' '));
      const ffmpeg = child_process.spawn(`"${options.ffmpegDir}/ffmpeg"`, args, { shell: true });
      ffmpeg.stdout.setEncoding('utf8');
      ffmpeg.stdout.on('data', async (data: string) => {
        const progress = ffmpegHelper.parseProgress(data);
        const percent = ffmpegHelper.progressPercent(progress.outTimeMs, options.duration * 1000000);
        stdout.write(`${ffmpegHelper.getProgressMessage(progress, percent)}\r`);
      });

      ffmpeg.stderr.setEncoding('utf8');
      ffmpeg.stderr.on('data', (data) => {
        stdout.write(data);
      });

      const cancelledJobChecker = createCancelChecker(
        () => options.canceledJobIds,
        ids => (options.canceledJobIds = ids),
        options.jobId,
        () => {
          isCancelled = true;
          ffmpeg.stdin.write('q');
          ffmpeg.stdin.end();
        },
        5000
      );

      ffmpeg.on('exit', (code: number) => {
        stdout.write('\n');
        clearInterval(cancelledJobChecker);
        if (isCancelled) {
          reject(RejectCode.JOB_CANCEL);
        } else if (code !== 0) {
          reject({ code, message: `FFmpeg exited with status code: ${code}` });
        } else {
          resolve();
        }
      });
    });
  }
}

export const videoSourceHelper = new VideoSourceHelper();