import { DateTime } from 'luxon';
import { stdout } from 'process';
import child_process from 'child_process';
import path from 'path';

import { IStorage } from '../resources/video/interfaces/storage.interface';
import { RcloneCommandOptions, RcloneFile, RcloneProgress } from '../common/interfaces';
import { stringHelper } from './string-helper.util';

export class RcloneHelper {
  createRcloneConfig(storage: IStorage) {
    if (storage.kind === 7 || storage.kind === 8) {
      return this.createS3RcloneConfig(storage);
    }
    const token = JSON.stringify({
      access_token: storage.accessToken,
      token_type: 'Bearer',
      refresh_token: storage.refreshToken,
      expiry: DateTime.fromJSDate(storage.expiry).toISO()
    });
    let newConfig = `[${storage._id}]\n`;
    if (storage.kind === 3) {
      newConfig += 'type = drive\n';
    }
    else {
      newConfig += 'type = onedrive\n';
    }
    newConfig += `client_id = ${storage.clientId}\n`;
    newConfig += `client_secret = ${storage.clientSecret}\n`;
    newConfig += `token = ${token}\n`;
    if (storage.kind === 3) {
      newConfig += `root_folder_id = ${storage.folderId}\n\n`;
    }
    if (storage.kind === 6) {
      const [driveId, folderId] = storage.folderId.split('#');
      folderId && (newConfig += `root_folder_id = ${storage.folderId}\n`);
      newConfig += `drive_id = ${driveId}\n`;
      newConfig += 'drive_type = business\n';
      newConfig += 'no_versions = true\n\n';
    }
    return newConfig;
  }

  private createS3RcloneConfig(storage: IStorage) {
    const { endpoint, bucket, folderPrefix } = this.parseS3PublicUrl(storage.publicUrl, storage.folderId);
    const baseRemoteName = `${storage._id}_s3`;
    let newConfig = `[${baseRemoteName}]\n`;
    newConfig += 'type = s3\n';
    newConfig += 'provider = Other\n';
    newConfig += `access_key_id = ${storage.clientId}\n`;
    newConfig += `secret_access_key = ${storage.clientSecret}\n`;
    newConfig += `endpoint = ${endpoint}\n`;
    newConfig += 'region = auto\n\n';
    newConfig += `[${storage._id}]\n`;
    newConfig += 'type = alias\n';
    newConfig += `remote = ${baseRemoteName}:${bucket}${folderPrefix}\n\n`;
    return newConfig;
  }

  private parseS3PublicUrl(publicUrl: string, folderId?: string) {
    const hasServicePath = publicUrl.includes(':service_path');
    const normalized = publicUrl.replace(':service_path', 's3').replace(':path', '');
    const url = new URL(normalized);
    const pathParts = url.pathname.split('/').filter(Boolean);
    let endpoint = `${url.protocol}//${url.host}`;
    if (hasServicePath) {
      endpoint += `/${pathParts[0]}/`;
      pathParts.shift();
    }
    const bucket = pathParts[0] || '';
    const folderPrefix = folderId ? `/${folderId}` : '';
    return { endpoint, bucket, folderPrefix };
  }

  downloadFile(configPath: string, rcloneDir: string, remote: string, folder: string, file: string,
    saveFolder: string, useFilter: boolean, logFn: (args: string[]) => void) {
    const filePath = path.posix.join(folder, file);
    const copyArgs = useFilter ?
      [`"${remote}:${folder}"`, saveFolder, '--include', `"${stringHelper.escapeRegExp(file)}"`] :
      [`"${remote}:${filePath}"`, saveFolder];
    const args: string[] = [
      '--ignore-checksum',
      '--config', `"${configPath}"`,
      '--low-level-retries', '5',
      '-v', '--use-json-log',
      '--stats', '3s',
      'copy', ...copyArgs
    ];
    logFn(args);
    //console.log('\x1b[36m%s\x1b[0m', 'rclone ' + args.join(' '));
    return new Promise<void>((resolve, reject) => {
      const rclone = child_process.spawn(`"${rcloneDir}/rclone"`, args, { shell: true });

      let errorMessage = '';
      rclone.stderr.setEncoding('utf8');
      rclone.stderr.on('data', (data) => {
        const progress = this.parseRcloneUploadProgress(data);
        if (progress)
          stdout.write(`${progress.msg}\r`);
        errorMessage = data;
      });

      rclone.on('exit', (code) => {
        stdout.write('\n');
        if (code === 0 || code === 9)
          resolve();
        else
          reject({ code: code, message: errorMessage })
      });
    });
  }

  async readRemoteFile(configPath: string, rcloneDir: string, remote: string, folder: string, file: string,
    logFn: (args: string[]) => void) {
    const filePath = path.posix.join(folder, file);
    const args: string[] = [
      '--config', `"${configPath}"`,
      'cat', `"${remote}:${filePath}"`
    ];
    logFn(args);
    return new Promise<string>((resolve, reject) => {
      const rclone = child_process.spawn(`"${rcloneDir}/rclone"`, args, { shell: true });
      let fileContent = '';
      let errorMessage = '';
      rclone.stdout.setEncoding('utf8');
      rclone.stdout.on('data', (data) => {
        fileContent += data;
      });
      rclone.stderr.setEncoding('utf8');
      rclone.stderr.on('data', (data) => {
        errorMessage += data + '\n';
      });

      rclone.on('exit', (code) => {
        if (code !== 0)
          reject({ code: code, message: errorMessage })
        else
          resolve(fileContent);
      });
    });
  }

  async deletePath(configPath: string, rcloneDir: string, remote: string, path: string, logFn: (args: string[]) => void) {
    const args: string[] = [
      '--config', `"${configPath}"`,
      'purge', `"${remote}:${path}"`
    ];
    logFn(args);
    const pathExist = await this.isPathExist(configPath, rcloneDir, remote, path);
    if (!pathExist) return;
    //console.log('\x1b[36m%s\x1b[0m', 'rclone ' + args.join(' '));
    return new Promise<void>((resolve, reject) => {
      const rclone = child_process.spawn(`"${rcloneDir}/rclone"`, args, { shell: true });
      let errorMessage = '';
      rclone.stderr.setEncoding('utf8');
      rclone.stderr.on('data', (data) => {
        errorMessage += data + '\n';
      });

      rclone.on('exit', (code) => {
        if (code === 0 || code === 9)
          resolve();
        else
          reject({ code: code, message: errorMessage })
      });
    });
  }

  async deleteFile(configPath: string, rcloneDir: string, remote: string, path: string, logFn: (args: string[]) => void) {
    const args: string[] = [
      '--config', `"${configPath}"`,
      'delete', `"${remote}:${path}"`
    ];
    logFn(args);
    const pathExist = await this.isPathExist(configPath, rcloneDir, remote, path);
    if (!pathExist) return;
    //console.log('\x1b[36m%s\x1b[0m', 'rclone ' + args.join(' '));
    return new Promise<void>((resolve, reject) => {
      const rclone = child_process.spawn(`"${rcloneDir}/rclone"`, args, { shell: true });
      let errorMessage = '';
      rclone.stderr.setEncoding('utf8');
      rclone.stderr.on('data', (data) => {
        errorMessage += data + '\n';
      });

      rclone.on('exit', (code) => {
        if (code === 0 || code === 9)
          resolve();
        else
          reject({ code: code, message: errorMessage })
      });
    });
  }

  async emptyPath(configPath: string, rcloneDir: string, remote: string, path: string, logFn: (args: string[]) => void,
    options: RcloneCommandOptions = {}) {
    const args: string[] = [
      '--config', `"${configPath}"`,
      'delete', `"${remote}:${path}"`,
      '--rmdirs'
    ];
    options.include && args.push('--include', options.include);
    options.exclude && args.push('--exclude', options.exclude);
    logFn(args);
    const pathExist = await this.isPathExist(configPath, rcloneDir, remote, path);
    if (!pathExist) return;
    return new Promise<void>((resolve, reject) => {
      const rclone = child_process.spawn(`"${rcloneDir}/rclone"`, args, { shell: true });
      let errorMessage = '';
      rclone.stderr.setEncoding('utf8');
      rclone.stderr.on('data', (data) => {
        errorMessage += data + '\n';
      });

      rclone.on('exit', (code) => {
        if (code === 0 || code === 9)
          resolve();
        else
          reject({ code: code, message: errorMessage })
      });
    });
  }

  deleteRemote(configPath: string, rcloneDir: string, remote: string, logFn: (args: string[]) => void) {
    const args: string[] = [
      '--config', `"${configPath}"`,
      'config', 'delete',
      remote
    ];
    logFn(args);
    //console.log('\x1b[36m%s\x1b[0m', 'rclone ' + args.join(' '));
    return new Promise<void>((resolve, reject) => {
      const rclone = child_process.spawn(`"${rcloneDir}/rclone"`, args, { shell: true });
      let errorMessage = '';
      rclone.stderr.setEncoding('utf8');
      rclone.stderr.on('data', (data) => {
        errorMessage += data + '\n';
      });

      rclone.on('exit', (code) => {
        if (code === 0 || code === 9)
          resolve();
        else
          reject({ code: code, message: errorMessage })
      });
    });
  }

  listRemoteJson(configPath: string, rcloneDir: string, remote: string, folder: string,
    options: RcloneCommandOptions = {}) {
    const args: string[] = [
      '--config', `"${configPath}"`,
      'lsjson', `"${remote}:${folder}"`
    ];
    options.dirsOnly && args.push('--dirs-only');
    options.filesOnly && args.push('--files-only');
    options.recursive && args.push('--recursive');
    options.include && args.push('--include', `"${options.include}"`);
    options.exclude && args.push('--exclude', `"${options.exclude}"`);
    //console.log('\x1b[36m%s\x1b[0m', 'rclone ' + args.join(' '));
    return new Promise<RcloneFile[]>((resolve, reject) => {
      const rclone = child_process.spawn(`"${rcloneDir}/rclone"`, args, { shell: true });
      let listJson = '';
      let errorMessage = '';
      rclone.stdout.setEncoding('utf8');
      rclone.stdout.on('data', (data) => {
        listJson += data;
      });
      rclone.stderr.setEncoding('utf8');
      rclone.stderr.on('data', (data) => {
        errorMessage += data + '\n';
      });

      rclone.on('exit', (code: number) => {
        if (code !== 0) {
          reject({ code: code, message: errorMessage });
        } else {
          const fileData = JSON.parse(listJson);
          resolve(fileData);
        }
      });
    });
  }

  isPathExist(configPath: string, rcloneDir: string, remote: string, path: string) {
    const args: string[] = [
      '--config', `"${configPath}"`,
      '--low-level-retries', '1',
      'lsd', `"${remote}:${path}"`
    ];
    //console.log('\x1b[36m%s\x1b[0m', 'rclone ' + args.join(' '));
    return new Promise<boolean>((resolve) => {
      const rclone = child_process.spawn(`"${rcloneDir}/rclone"`, args, { shell: true });
      rclone.on('exit', (code: number) => {
        if (code !== 0) {
          resolve(false);
        } else {
          resolve(true);
        }
      });
    });
  }

  mkdirRemote(configPath: string, rcloneDir: string, remote: string, path: string) {
    const args: string[] = [
      '--config', `"${configPath}"`,
      '--low-level-retries', '5',
      'mkdir', `"${remote}:${path}"`
    ];
    //console.log('\x1b[36m%s\x1b[0m', 'rclone ' + args.join(' '));
    return new Promise<boolean>((resolve) => {
      const rclone = child_process.spawn(`"${rcloneDir}/rclone"`, args, { shell: true });
      rclone.on('exit', (code: number) => {
        if (code !== 0) {
          resolve(false);
        } else {
          resolve(true);
        }
      });
    });
  }

  findAllRemotes(configPath: string, rcloneDir: string) {
    const args: string[] = [
      '--config', `"${configPath}"`,
      'listremotes'
    ];
    return new Promise<string[]>((resolve, reject) => {
      const rclone = child_process.spawn(`"${rcloneDir}/rclone"`, args, { shell: true });

      let remoteListString = '';
      let errorMessage = '';

      rclone.stdout.setEncoding('utf8');
      rclone.stdout.on('data', (data) => {
        remoteListString += data;
      });

      rclone.stderr.setEncoding('utf8');
      rclone.stderr.on('data', (data) => {
        errorMessage += data + '\n';
      });

      rclone.on('exit', (code: number) => {
        if (code !== 0) {
          reject({ code: code, message: errorMessage });
        } else {
          const remoteList = remoteListString.split('\n').filter(r => !!r);
          resolve(remoteList);
        }
      });
    });
  }

  async refreshRemoteTokens(configPath: string, rcloneDir: string, remotes: string[], logFn: (args: string[]) => void) {
    const allowedTypes = ['onedrive'];
    const remoteTypes = await this.getRemoteTypes(configPath, rcloneDir);
    for (let i = 0; i < remotes.length; i++) {
      const remote = remotes[i];
      const remoteType = remoteTypes.get(remote);
      if (!remoteType || !allowedTypes.includes(remoteType)) {
        continue;
      }
      const args: string[] = [
        '--config', `"${configPath}"`,
        'about', remote
      ];
      logFn(args);
      await new Promise<void>((resolve, reject) => {
        const rclone = child_process.spawn(`"${rcloneDir}/rclone"`, args, { shell: true });
        let errorMessage = '';

        rclone.stderr.setEncoding('utf8');
        rclone.stderr.on('data', (data) => {
          errorMessage += data + '\n';
        });

        rclone.on('exit', (code: number) => {
          if (code !== 0) {
            reject({ code: code, message: errorMessage });
          } else {
            resolve();
          }
        });
      });
    }
  }

  private getRemoteTypes(configPath: string, rcloneDir: string) {
    const args: string[] = [
      '--config', `"${configPath}"`,
      'listremotes', '--long'
    ];
    return new Promise<Map<string, string>>((resolve, reject) => {
      const rclone = child_process.spawn(`"${rcloneDir}/rclone"`, args, { shell: true });

      let remoteListString = '';
      let errorMessage = '';

      rclone.stdout.setEncoding('utf8');
      rclone.stdout.on('data', (data) => {
        remoteListString += data;
      });

      rclone.stderr.setEncoding('utf8');
      rclone.stderr.on('data', (data) => {
        errorMessage += data + '\n';
      });

      rclone.on('exit', (code: number) => {
        if (code !== 0) {
          reject({ code: code, message: errorMessage });
        } else {
          const remoteTypes = new Map<string, string>();
          const lines = remoteListString.split('\n').filter(r => !!r);
          for (const line of lines) {
            const [remote, type] = line.split(':').map(s => s.trim());
            if (remote && type) {
              remoteTypes.set(`${remote}:`, type);
            }
          }
          resolve(remoteTypes);
        }
      });
    });
  }

  parseRcloneUploadProgress(data: string) {
    let logJson: RcloneProgress;
    const cleanData = data.replace(new RegExp('\r|\n|\t', 'g'), ' ');
    try {
      logJson = JSON.parse(cleanData);
    } catch {
      console.log(cleanData);
      return null;
    }
    if (logJson.msg)
      logJson.msg = logJson.msg
        .replace(new RegExp('\r|\n|\t', 'g'), ' ')
        .replace(/ +(?= )/g, '')
        .replace(/Checking: Transferring: .+/g, '')
        .replace(/Transferring: .+/g, '')
        .replace(/Deleted: .+/g, '');
    return logJson;
  }
}

export const rcloneHelper = new RcloneHelper();