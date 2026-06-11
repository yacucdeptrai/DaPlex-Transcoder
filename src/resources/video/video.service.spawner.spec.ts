import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { getQueueToken } from '@nestjs/bullmq';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import child_process from 'child_process';

import { VideoService } from './video.service';
import { EncodingArgsService } from './encoding-args.service';
import { QualityResolverService } from './quality-resolver.service';
import { ProcessSpawnerService } from './process-spawner.service';
import { CodecPresetRegistry } from './codec-preset.registry';
import { RcloneService } from './rclone.service';
import { RejectCode, TaskQueue } from '../../enums';
import { DaplexApiService } from '../../common/modules/daplex-api';
import { TranscoderApiService } from '../../common/modules/transcoder-api';

/**
 * Characterization tests for the child_process.spawn helpers slated to become
 * ProcessSpawnerService (Plan 6.10): encodeMedia (ffmpeg), packageMedia (MP4Box),
 * uploadMedia (rclone move), findUploadedFiles (rclone lsjson).
 *
 * These pin the EXACT spawn contract today — binary + args + { shell: true }, the
 * exit-code -> resolve/reject mapping (including the RejectCode.* sentinels), and
 * the cancel path that flips the promise to RejectCode.JOB_CANCEL. They are
 * recorded GREEN while the methods still live on VideoService; after extraction
 * `target` is repointed to ProcessSpawnerService with these names unchanged,
 * proving the moved spawn behavior is identical.
 *
 * child_process.spawn is ALWAYS mocked — no real ffmpeg/MP4Box/rclone is spawned.
 */

// Minimal fake ChildProcess. stdout/stderr 'data' chunks are delivered
// synchronously when the listener registers; 'exit' fires on a microtask so the
// promise executor finishes wiring its handlers first (mirrors real spawn order).
function fakeProc(code: number | null, opts: { stdoutChunks?: string[]; stderrChunks?: string[] } = {}) {
  const stdin = { write: jest.fn(), end: jest.fn() };
  const proc: any = {
    stdin,
    kill: jest.fn(),
    stdout: {
      setEncoding: jest.fn(),
      on: (event: string, cb: (data: string) => void) => {
        if (event === 'data') (opts.stdoutChunks ?? []).forEach((c) => cb(c));
      }
    },
    stderr: {
      setEncoding: jest.fn(),
      on: (event: string, cb: (data: string) => void) => {
        if (event === 'data') (opts.stderrChunks ?? []).forEach((c) => cb(c));
      }
    },
    on: (event: string, cb: (code: number | null) => void) => {
      if (event === 'exit') queueMicrotask(() => cb(code));
    }
  };
  return proc;
}

// A child process whose 'exit' never fires on its own, so the test can drive the
// cancel-checker interval (5000ms / 500ms) before completing the run.
function pendingProc() {
  const stdin = { write: jest.fn(), end: jest.fn() };
  let exitCb: ((code: number | null) => void) | null = null;
  const proc: any = {
    stdin,
    kill: jest.fn(),
    stdout: { setEncoding: jest.fn(), on: jest.fn() },
    stderr: { setEncoding: jest.fn(), on: jest.fn() },
    on: (event: string, cb: (code: number | null) => void) => {
      if (event === 'exit') exitCb = cb;
    },
    fireExit: (code: number | null) => exitCb && exitCb(code)
  };
  return proc;
}

describe('VideoService process spawners (characterization)', () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let target: any;
  let spawnSpy: jest.SpyInstance;
  let stdoutSpy: jest.SpyInstance;

  const configValues: Record<string, string | undefined> = {
    FFMPEG_DIR: '/opt/ffmpeg',
    MP4BOX_DIR: '/opt/gpac',
    RCLONE_DIR: '/opt/rclone',
    RCLONE_CONFIG_FILE: '/config/rclone.conf'
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        VideoService,
        EncodingArgsService,
        QualityResolverService,
        ProcessSpawnerService,
        CodecPresetRegistry,
        RcloneService,
        {
          provide: WINSTON_MODULE_PROVIDER,
          useValue: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn(), notice: jest.fn() }
        },
        { provide: getQueueToken(TaskQueue.VIDEO_TRANSCODE_RESULT), useValue: { add: jest.fn(), remove: jest.fn() } },
        { provide: ConfigService, useValue: { get: jest.fn((key: string) => configValues[key]) } },
        { provide: DaplexApiService, useValue: {} },
        { provide: TranscoderApiService, useValue: {} }
      ]
    }).compile();

    target = module.get<VideoService>(VideoService);
    spawnSpy = jest.spyOn(child_process, 'spawn');
    // Suppress the progress writes so check-console / clean output is preserved.
    stdoutSpy = jest.spyOn(process.stdout, 'write').mockImplementation(() => true);
  });

  afterEach(() => {
    jest.restoreAllMocks();
    jest.useRealTimers();
  });

  // ---------------------------------------------------------------------------
  // encodeMedia (ffmpeg)
  // ---------------------------------------------------------------------------
  describe('encodeMedia (ffmpeg)', () => {
    it('spawns the quoted ffmpeg binary with the given args and shell:true', async () => {
      spawnSpy.mockReturnValue(fakeProc(0));
      await target.encodeMedia(['-i', 'in.mkv', 'out.mp4'], 100, 'job-1');
      expect(spawnSpy).toHaveBeenCalledTimes(1);
      expect(spawnSpy).toHaveBeenCalledWith('"/opt/ffmpeg/ffmpeg"', ['-i', 'in.mkv', 'out.mp4'], { shell: true });
    });

    it('resolves on exit code 0', async () => {
      spawnSpy.mockReturnValue(fakeProc(0));
      await expect(target.encodeMedia([], 100, 'job-1')).resolves.toBeUndefined();
    });

    it('rejects with { code, message } on a non-zero exit code', async () => {
      spawnSpy.mockReturnValue(fakeProc(1));
      await expect(target.encodeMedia([], 100, 'job-1')).rejects.toEqual({
        code: 1,
        message: 'FFmpeg exited with status code: 1'
      });
    });

    it('rejects with RejectCode.JOB_CANCEL when the job is cancelled mid-run', async () => {
      jest.useFakeTimers();
      const proc = pendingProc();
      spawnSpy.mockReturnValue(proc);

      const promise = target.encodeMedia([], 100, 7);
      const assertion = expect(promise).rejects.toBe(RejectCode.JOB_CANCEL);

      // Cancel job 7, then let the 5s cancel checker fire -> writes 'q', ends stdin.
      target.addToCanceled({ id: 7 });
      jest.advanceTimersByTime(5000);
      expect(proc.stdin.write).toHaveBeenCalledWith('q');
      expect(proc.stdin.end).toHaveBeenCalled();

      // ffmpeg then exits; isCancelled flag makes the promise reject JOB_CANCEL.
      proc.fireExit(0);
      await assertion;
    });

    // The retry/timeout checkers read closure-captured instance state
    // (CanRetryEncoding / RetryEncoding). The extraction must preserve this exact
    // contract via getters/setters, so it is pinned here on the only spawn site
    // that wires all three checkers.
    it('does NOT arm the retry checker while CanRetryEncoding is false (default)', async () => {
      jest.useFakeTimers();
      const proc = pendingProc();
      spawnSpy.mockReturnValue(proc);

      const promise = target.encodeMedia([], 100, 'job-1');
      // setRetryEncoding flips RetryEncoding, but the checker was never armed.
      target.setRetryEncoding();
      jest.advanceTimersByTime(5000);
      expect(proc.kill).not.toHaveBeenCalled();

      // Normal completion still resolves — no retry path taken.
      proc.fireExit(0);
      await expect(promise).resolves.toBeUndefined();
    });

    it('rejects RejectCode.RETRY_ENCODING when armed and setRetryEncoding fires the checker', async () => {
      jest.useFakeTimers();
      const proc = pendingProc();
      spawnSpy.mockReturnValue(proc);

      // Arm the retry checker (set by splitAndEncodeVideo orchestration at runtime).
      target.CanRetryEncoding = true;

      const promise = target.encodeMedia([], 100, 'job-1');
      const assertion = expect(promise).rejects.toBe(RejectCode.RETRY_ENCODING);

      target.setRetryEncoding();
      jest.advanceTimersByTime(5000);
      expect(proc.kill).toHaveBeenCalledWith('SIGINT');
      expect(proc.kill).toHaveBeenCalledWith('SIGTERM');
      // The checker consumes the flag (resets RetryEncoding to false).
      expect(target.RetryEncoding).toBe(false);

      proc.fireExit(0);
      await assertion;
    });

    it('rejects RejectCode.ENCODING_TIMEOUT after two timeout ticks with no progress change', async () => {
      jest.useFakeTimers();
      const proc = pendingProc();
      spawnSpy.mockReturnValue(proc);

      const promise = target.encodeMedia([], 100, 'job-1');
      const assertion = expect(promise).rejects.toBe(RejectCode.ENCODING_TIMEOUT);

      // First 10-min tick only arms isProgressTimeout; the second tick kills ffmpeg.
      jest.advanceTimersByTime(600_000);
      expect(proc.kill).not.toHaveBeenCalled();
      jest.advanceTimersByTime(600_000);
      expect(proc.kill).toHaveBeenCalledWith('SIGINT');
      expect(proc.kill).toHaveBeenCalledWith('SIGTERM');

      proc.fireExit(0);
      await assertion;
    });
  });

  // ---------------------------------------------------------------------------
  // packageMedia (MP4Box)
  // ---------------------------------------------------------------------------
  describe('packageMedia (MP4Box)', () => {
    it('spawns the quoted MP4Box binary with the given args and shell:true', async () => {
      spawnSpy.mockReturnValue(fakeProc(0));
      await target.packageMedia(['-dash', '6000'], 'job-1');
      expect(spawnSpy).toHaveBeenCalledWith('"/opt/gpac/MP4Box"', ['-dash', '6000'], { shell: true });
    });

    it('resolves on exit code 0', async () => {
      spawnSpy.mockReturnValue(fakeProc(0));
      await expect(target.packageMedia([], 'job-1')).resolves.toBeUndefined();
    });

    it('rejects with a status-code string on a non-zero exit code', async () => {
      spawnSpy.mockReturnValue(fakeProc(2));
      await expect(target.packageMedia([], 'job-1')).rejects.toBe('MP4Box exited with status code: 2');
    });

    it('rejects with RejectCode.JOB_CANCEL when cancelled mid-run', async () => {
      jest.useFakeTimers();
      const proc = pendingProc();
      spawnSpy.mockReturnValue(proc);

      const promise = target.packageMedia([], 11);
      const assertion = expect(promise).rejects.toBe(RejectCode.JOB_CANCEL);

      target.addToCanceled({ id: 11 });
      jest.advanceTimersByTime(5000);
      expect(proc.kill).toHaveBeenCalledWith('SIGINT');

      proc.fireExit(0);
      await assertion;
    });
  });

  // ---------------------------------------------------------------------------
  // uploadMedia (rclone move)
  // ---------------------------------------------------------------------------
  describe('uploadMedia (rclone move)', () => {
    it('spawns the quoted rclone binary with the given args and shell:true', async () => {
      spawnSpy.mockReturnValue(fakeProc(0));
      await target.uploadMedia(['move', 'a', 'b'], 'job-1');
      expect(spawnSpy).toHaveBeenCalledWith('"/opt/rclone/rclone"', ['move', 'a', 'b'], { shell: true });
    });

    it('resolves on exit code 0', async () => {
      spawnSpy.mockReturnValue(fakeProc(0));
      await expect(target.uploadMedia([], 'job-1')).resolves.toBeUndefined();
    });

    it('rejects with a status-code string on a non-zero exit code', async () => {
      spawnSpy.mockReturnValue(fakeProc(5));
      await expect(target.uploadMedia([], 'job-1')).rejects.toBe('Rclone exited with status code: 5');
    });

    it('rejects with RejectCode.JOB_CANCEL when cancelled mid-run', async () => {
      jest.useFakeTimers();
      const proc = pendingProc();
      spawnSpy.mockReturnValue(proc);

      const promise = target.uploadMedia([], 22);
      const assertion = expect(promise).rejects.toBe(RejectCode.JOB_CANCEL);

      target.addToCanceled({ id: 22 });
      jest.advanceTimersByTime(5000);
      expect(proc.kill).toHaveBeenCalledWith('SIGINT');

      proc.fireExit(0);
      await assertion;
    });
  });

  // ---------------------------------------------------------------------------
  // findUploadedFiles (rclone lsjson)
  // ---------------------------------------------------------------------------
  describe('findUploadedFiles (rclone lsjson)', () => {
    it('spawns rclone lsjson with config + recursive + files-only args', async () => {
      spawnSpy.mockReturnValue(fakeProc(0, { stdoutChunks: ['[]'] }));
      await target.findUploadedFiles('gd', '42', 'job-1');
      expect(spawnSpy).toHaveBeenCalledWith(
        '"/opt/rclone/rclone"',
        ['--config', '/config/rclone.conf', 'lsjson', 'gd:42', '--recursive', '--files-only'],
        { shell: true }
      );
    });

    it('appends an --exclude arg when an exclude pattern is given', async () => {
      spawnSpy.mockReturnValue(fakeProc(0, { stdoutChunks: ['[]'] }));
      await target.findUploadedFiles('gd', '42', 'job-1', '*.tmp');
      expect(spawnSpy).toHaveBeenCalledWith(
        '"/opt/rclone/rclone"',
        ['--config', '/config/rclone.conf', 'lsjson', 'gd:42', '--recursive', '--files-only', '--exclude', '*.tmp'],
        { shell: true }
      );
    });

    it('parses and returns the accumulated lsjson stdout on exit code 0', async () => {
      const files = [{ Path: '1/source_1080.mp4' }, { Path: '1/source_720.mp4' }];
      spawnSpy.mockReturnValue(fakeProc(0, { stdoutChunks: [JSON.stringify(files)] }));
      await expect(target.findUploadedFiles('gd', '42', 'job-1')).resolves.toEqual(files);
    });

    it('resolves to an empty array on exit code 3 (directory not found)', async () => {
      spawnSpy.mockReturnValue(fakeProc(3));
      await expect(target.findUploadedFiles('gd', '42', 'job-1')).resolves.toEqual([]);
    });

    it('rejects with a listing-error string on other non-zero exit codes', async () => {
      spawnSpy.mockReturnValue(fakeProc(1));
      await expect(target.findUploadedFiles('gd', '42', 'job-1')).rejects.toBe(
        'Error listing files, rclone exited with status code: 1'
      );
    });

    it('rejects with RejectCode.JOB_CANCEL when cancelled mid-listing (500ms checker)', async () => {
      jest.useFakeTimers();
      const proc = pendingProc();
      spawnSpy.mockReturnValue(proc);

      const promise = target.findUploadedFiles('gd', '42', 33);
      const assertion = expect(promise).rejects.toBe(RejectCode.JOB_CANCEL);

      target.addToCanceled({ id: 33 });
      jest.advanceTimersByTime(500);
      expect(proc.kill).toHaveBeenCalledWith('SIGINT');

      proc.fireExit(0);
      await assertion;
    });
  });

  // ---------------------------------------------------------------------------
  // addToCanceled (instance-state contract the spawner checkers depend on)
  // ---------------------------------------------------------------------------
  describe('addToCanceled (cancel-state seam)', () => {
    it('pushes a single id and returns the jobData unchanged', () => {
      const jobData = { id: 5 };
      expect(target.addToCanceled(jobData)).toBe(jobData);
      expect(target.CanceledJobIds).toContain(5);
    });

    it('spreads multiple ids onto the cancelled list', () => {
      target.addToCanceled({ ids: [1, 2, 3] });
      expect(target.CanceledJobIds).toEqual(expect.arrayContaining([1, 2, 3]));
    });
  });
});
