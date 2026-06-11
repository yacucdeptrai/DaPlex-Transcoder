import { Inject, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';
import { stdout } from 'process';
import child_process from 'child_process';

import { RejectCode } from '../../enums';
import { ffmpegHelper, rcloneHelper, isEqualShallow, createCancelChecker } from '../../utils';
import { Progress } from '../../common/entities';
import { RcloneFile } from '../../common/interfaces';

/**
 * Cancel/retry state stays owned by VideoService; the spawner reads it live
 * through these accessors so a cancel signal raised mid-run reaches the
 * in-flight ffmpeg/rclone process.
 */
export interface SpawnStateAccessors {
  getCanceledJobIds: () => (string | number)[];
  setCanceledJobIds: (ids: (string | number)[]) => void;
  getRetryEncoding: () => boolean;
  setRetryEncoding: (value: boolean) => void;
  getCanRetryEncoding: () => boolean;
}

/**
 * Owns every child_process.spawn site of the transcode pipeline (ffmpeg,
 * MP4Box, rclone) plus the cancel/retry/timeout interval checkers that drive
 * them. VideoService delegates to this and registers its cancel/retry state
 * via setStateAccessors.
 */
@Injectable()
export class ProcessSpawnerService {
  private state: SpawnStateAccessors;

  constructor(
    @Inject(WINSTON_MODULE_PROVIDER) private readonly logger: Logger,
    private configService: ConfigService
  ) {}

  setStateAccessors(state: SpawnStateAccessors) {
    this.state = state;
  }

  encodeMedia(args: string[], videoDuration: number, jobId: string | number) {
    return new Promise<void>((resolve, reject) => {
      let isCancelled = false;
      let isRetryEncoding = false;
      let isProgressTimeout = false;
      let lastProgress: Progress | null = null;

      this.logger.info('ffmpeg ' + args.join(' '));
      const ffmpeg = child_process.spawn(`"${this.configService.get<string>('FFMPEG_DIR')}/ffmpeg"`, args, {
        shell: true
      });

      ffmpeg.stdout.setEncoding('utf8');
      ffmpeg.stdout.on('data', async (data: string) => {
        const progress = ffmpegHelper.parseProgress(data);
        if (!isEqualShallow(lastProgress, progress)) isProgressTimeout = false;
        lastProgress = { ...progress };
        const percent = ffmpegHelper.progressPercent(progress.outTimeMs, videoDuration * 1000000);
        stdout.write(`${ffmpegHelper.getProgressMessage(progress, percent)}\r`);
      });

      ffmpeg.stderr.setEncoding('utf8');
      ffmpeg.stderr.on('data', (data) => {
        stdout.write(data);
      });

      const cancelledJobChecker = this.createCancelJobChecker(jobId, () => {
        isCancelled = true;
        ffmpeg.stdin.write('q');
        ffmpeg.stdin.end();
      });

      const retryEncodingChecker = this.createRetryEncodingChecker(() => {
        isRetryEncoding = true;
        ffmpeg.kill('SIGINT');
        ffmpeg.kill('SIGTERM');
      });

      const progressTimeoutChecker = this.createTimeoutChecker(() => {
        if (isProgressTimeout) {
          ffmpeg.kill('SIGINT');
          ffmpeg.kill('SIGTERM');
          return;
        }
        isProgressTimeout = true;
      });

      ffmpeg.on('exit', (code: number) => {
        stdout.write('\n');
        clearInterval(cancelledJobChecker);
        clearInterval(retryEncodingChecker);
        clearInterval(progressTimeoutChecker);
        if (isCancelled) {
          reject(RejectCode.JOB_CANCEL);
        } else if (isRetryEncoding) {
          reject(RejectCode.RETRY_ENCODING);
        } else if (isProgressTimeout) {
          reject(RejectCode.ENCODING_TIMEOUT);
        } else if (code !== 0) {
          reject({ code, message: `FFmpeg exited with status code: ${code}` });
        } else {
          resolve();
        }
      });
    });
  }

  packageMedia(args: string[], jobId: string | number) {
    return new Promise<void>((resolve, reject) => {
      let isCancelled = false;

      this.logger.info('MP4Box ' + args.join(' '));
      const mp4box = child_process.spawn(`"${this.configService.get<string>('MP4BOX_DIR')}/MP4Box"`, args, {
        shell: true
      });

      mp4box.stderr.setEncoding('utf8');
      mp4box.stderr.on('data', (data) => {
        stdout.write(data);
      });

      const cancelledJobChecker = this.createCancelJobChecker(jobId, () => {
        isCancelled = true;
        mp4box.kill('SIGINT'); // Stop key
      });

      mp4box.on('exit', (code: number) => {
        stdout.write('\n');
        clearInterval(cancelledJobChecker);
        if (isCancelled) {
          reject(RejectCode.JOB_CANCEL);
        } else if (code !== 0) {
          reject(`MP4Box exited with status code: ${code}`);
        } else {
          resolve();
        }
      });
    });
  }

  uploadMedia(args: string[], jobId: string | number) {
    return new Promise<void>((resolve, reject) => {
      let isCancelled = false;

      this.logger.info('rclone ' + args.join(' '));
      const rclone = child_process.spawn(`"${this.configService.get<string>('RCLONE_DIR')}/rclone"`, args, {
        shell: true
      });

      rclone.stderr.setEncoding('utf8');
      rclone.stderr.on('data', (data) => {
        const progress = rcloneHelper.parseRcloneUploadProgress(data);
        if (progress) stdout.write(`${progress.msg}\r`);
      });

      const cancelledJobChecker = this.createCancelJobChecker(jobId, () => {
        isCancelled = true;
        rclone.kill('SIGINT'); // Stop key
      });

      rclone.on('exit', (code: number) => {
        stdout.write('\n');
        clearInterval(cancelledJobChecker);
        if (isCancelled) {
          reject(RejectCode.JOB_CANCEL);
        } else if (code !== 0) {
          reject(`Rclone exited with status code: ${code}`);
        } else {
          resolve();
        }
      });
    });
  }

  findUploadedFiles(remote: string, parentFolder: string, jobId: string | number, exclude?: string) {
    const rcloneConfigFile = this.configService.get<string>('RCLONE_CONFIG_FILE');
    const args: string[] = [
      '--config',
      rcloneConfigFile,
      'lsjson',
      `${remote}:${parentFolder}`,
      '--recursive',
      '--files-only'
    ];
    if (exclude) {
      args.push('--exclude', exclude);
    }
    return new Promise<RcloneFile[]>((resolve, reject) => {
      let isCancelled = false;
      this.logger.info('rclone ' + args.join(' '));
      const rclone = child_process.spawn(`"${this.configService.get<string>('RCLONE_DIR')}/rclone"`, args, {
        shell: true
      });

      let listJson = '';

      rclone.stdout.setEncoding('utf8');
      rclone.stdout.on('data', (data) => {
        listJson += data;
      });

      rclone.stderr.setEncoding('utf8');
      rclone.stderr.on('data', (data) => {
        stdout.write(data);
      });

      const cancelledJobChecker = this.createCancelJobChecker(
        jobId,
        () => {
          isCancelled = true;
          rclone.kill('SIGINT');
        },
        500
      );

      rclone.on('exit', (code: number) => {
        clearInterval(cancelledJobChecker);
        if (isCancelled) {
          reject(RejectCode.JOB_CANCEL);
        } else if (code === 3) {
          // Return an empty array if directory not found
          resolve([]);
        } else if (code !== 0) {
          reject(`Error listing files, rclone exited with status code: ${code}`);
        } else {
          const fileData = JSON.parse(listJson);
          resolve(fileData);
        }
      });
    });
  }

  private createCancelJobChecker(jobId: string | number, exec: () => void, ms: number = 5000) {
    return createCancelChecker(
      () => this.state.getCanceledJobIds(),
      (ids) => this.state.setCanceledJobIds(ids),
      jobId,
      exec,
      ms
    );
  }

  private createRetryEncodingChecker(exec: () => void, ms: number = 5000) {
    if (!this.state.getCanRetryEncoding()) return null;
    return setInterval(() => {
      if (!this.state.getRetryEncoding()) return;
      this.state.setRetryEncoding(false);
      // Exec callback
      exec();
    }, ms);
  }

  private createTimeoutChecker(exec: () => void, ms: number = 600_000) {
    return setInterval(() => {
      exec();
    }, ms);
  }
}
