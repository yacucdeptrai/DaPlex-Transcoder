import child_process from 'child_process';

import { rcloneHelper } from './rclone.util';

// Minimal fake ChildProcess: stderr chunks are delivered synchronously when the
// 'data' listener is registered; 'exit' fires on a microtask (after the source's
// Promise executor has wired up its handlers), mirroring real spawn ordering.
function fakeProc(code: number | null, stderrChunks: string[] = []) {
  return {
    stderr: {
      setEncoding: jest.fn(),
      on: (event: string, cb: (data: string) => void) => {
        if (event === 'data') stderrChunks.forEach((chunk) => cb(chunk));
      }
    },
    on: (event: string, cb: (code: number | null) => void) => {
      if (event === 'exit') queueMicrotask(() => cb(code));
    }
  };
}

const RCLONE_DIR = '/opt/rclone';
const RCLONE_BIN = `"${RCLONE_DIR}/rclone"`;
const SHELL_OPTS = { shell: true };
const noop = () => undefined;

describe('RcloneHelper void rclone commands (Group-A)', () => {
  let spawnSpy: jest.SpyInstance;

  beforeEach(() => {
    spawnSpy = jest.spyOn(child_process, 'spawn');
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  // deleteRemote has no isPathExist pre-check, so it exercises the shared
  // void-spawn block (resolve on 0/9, reject on other with appended stderr)
  // in isolation across every exit-code path.
  describe('deleteRemote (exercises the shared void-spawn lifecycle)', () => {
    it('spawns the rclone binary with config-delete args and shell option', async () => {
      spawnSpy.mockReturnValue(fakeProc(0) as any);

      await rcloneHelper.deleteRemote('cfg.conf', RCLONE_DIR, 'remote:', noop);

      expect(spawnSpy).toHaveBeenCalledTimes(1);
      expect(spawnSpy).toHaveBeenCalledWith(
        RCLONE_BIN,
        ['--config', '"cfg.conf"', 'config', 'delete', 'remote:'],
        SHELL_OPTS
      );
    });

    it('resolves on exit code 0', async () => {
      spawnSpy.mockReturnValue(fakeProc(0) as any);
      await expect(rcloneHelper.deleteRemote('cfg.conf', RCLONE_DIR, 'remote:', noop)).resolves.toBeUndefined();
    });

    it('resolves on exit code 9 (rclone "directory not found" treated as success)', async () => {
      spawnSpy.mockReturnValue(fakeProc(9) as any);
      await expect(rcloneHelper.deleteRemote('cfg.conf', RCLONE_DIR, 'remote:', noop)).resolves.toBeUndefined();
    });

    it('rejects on other exit codes with the newline-appended stderr message', async () => {
      spawnSpy.mockReturnValue(fakeProc(1, ['boom', 'more']) as any);
      await expect(rcloneHelper.deleteRemote('cfg.conf', RCLONE_DIR, 'remote:', noop)).rejects.toEqual({
        code: 1,
        message: 'boom\nmore\n'
      });
    });

    it('passes the logFn the args array', async () => {
      spawnSpy.mockReturnValue(fakeProc(0) as any);
      const logFn = jest.fn();

      await rcloneHelper.deleteRemote('cfg.conf', RCLONE_DIR, 'remote:', logFn);

      expect(logFn).toHaveBeenCalledWith(['--config', '"cfg.conf"', 'config', 'delete', 'remote:']);
    });
  });

  // deletePath / deleteFile / emptyPath: same void-spawn block, but gated by an
  // isPathExist pre-check. Spy isPathExist to isolate each method's own args +
  // short-circuit behaviour.
  describe('deletePath', () => {
    it('skips spawning when the path does not exist', async () => {
      jest.spyOn(rcloneHelper, 'isPathExist').mockResolvedValue(false);

      await expect(rcloneHelper.deletePath('cfg.conf', RCLONE_DIR, 'remote:', 'a/b', noop)).resolves.toBeUndefined();
      expect(spawnSpy).not.toHaveBeenCalled();
    });

    it('purges the path with the expected args when it exists', async () => {
      jest.spyOn(rcloneHelper, 'isPathExist').mockResolvedValue(true);
      spawnSpy.mockReturnValue(fakeProc(0) as any);

      await expect(rcloneHelper.deletePath('cfg.conf', RCLONE_DIR, 'remote:', 'a/b', noop)).resolves.toBeUndefined();
      expect(spawnSpy).toHaveBeenCalledWith(
        RCLONE_BIN,
        ['--config', '"cfg.conf"', 'purge', '"remote::a/b"'],
        SHELL_OPTS
      );
    });

    it('rejects with appended stderr when the command fails', async () => {
      jest.spyOn(rcloneHelper, 'isPathExist').mockResolvedValue(true);
      spawnSpy.mockReturnValue(fakeProc(2, ['nope']) as any);

      await expect(rcloneHelper.deletePath('cfg.conf', RCLONE_DIR, 'remote:', 'a/b', noop)).rejects.toEqual({
        code: 2,
        message: 'nope\n'
      });
    });
  });

  describe('deleteFile', () => {
    it('skips spawning when the path does not exist', async () => {
      jest.spyOn(rcloneHelper, 'isPathExist').mockResolvedValue(false);

      await expect(
        rcloneHelper.deleteFile('cfg.conf', RCLONE_DIR, 'remote:', 'a/b.mp4', noop)
      ).resolves.toBeUndefined();
      expect(spawnSpy).not.toHaveBeenCalled();
    });

    it('deletes the file with the expected args when it exists', async () => {
      jest.spyOn(rcloneHelper, 'isPathExist').mockResolvedValue(true);
      spawnSpy.mockReturnValue(fakeProc(0) as any);

      await expect(
        rcloneHelper.deleteFile('cfg.conf', RCLONE_DIR, 'remote:', 'a/b.mp4', noop)
      ).resolves.toBeUndefined();
      expect(spawnSpy).toHaveBeenCalledWith(
        RCLONE_BIN,
        ['--config', '"cfg.conf"', 'delete', '"remote::a/b.mp4"'],
        SHELL_OPTS
      );
    });
  });

  describe('emptyPath', () => {
    it('skips spawning when the path does not exist', async () => {
      jest.spyOn(rcloneHelper, 'isPathExist').mockResolvedValue(false);

      await expect(rcloneHelper.emptyPath('cfg.conf', RCLONE_DIR, 'remote:', 'a/b', noop)).resolves.toBeUndefined();
      expect(spawnSpy).not.toHaveBeenCalled();
    });

    it('deletes with --rmdirs and the expected args when it exists', async () => {
      jest.spyOn(rcloneHelper, 'isPathExist').mockResolvedValue(true);
      spawnSpy.mockReturnValue(fakeProc(0) as any);

      await expect(rcloneHelper.emptyPath('cfg.conf', RCLONE_DIR, 'remote:', 'a/b', noop)).resolves.toBeUndefined();
      expect(spawnSpy).toHaveBeenCalledWith(
        RCLONE_BIN,
        ['--config', '"cfg.conf"', 'delete', '"remote::a/b"', '--rmdirs'],
        SHELL_OPTS
      );
    });

    it('appends include/exclude filters when provided', async () => {
      jest.spyOn(rcloneHelper, 'isPathExist').mockResolvedValue(true);
      spawnSpy.mockReturnValue(fakeProc(0) as any);

      await rcloneHelper.emptyPath('cfg.conf', RCLONE_DIR, 'remote:', 'a/b', noop, {
        include: '*.tmp',
        exclude: '*.keep'
      });

      expect(spawnSpy).toHaveBeenCalledWith(
        RCLONE_BIN,
        ['--config', '"cfg.conf"', 'delete', '"remote::a/b"', '--rmdirs', '--include', '*.tmp', '--exclude', '*.keep'],
        SHELL_OPTS
      );
    });
  });
});
