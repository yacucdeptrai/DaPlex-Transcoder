import { DateTime } from 'luxon';

import { rcloneHelper } from './rclone.util';
import { IStorage } from '../resources/video/interfaces/storage.interface';

/**
 * Characterization tests for the rclone config-file builders that dispatch on
 * storage.kind: createRcloneConfig (drive=3, onedrive=others, onedrive-business=6,
 * s3=7/8) and the private createS3RcloneConfig / parseS3PublicUrl it delegates to.
 *
 * These pin the EXACT config-file text produced today for each provider kind so a
 * split that moves config assembly into RcloneService cannot silently change the
 * generated config (a security-sensitive surface flagged in the analyst brief —
 * the kind dispatch must be byte-stable). Pure string assembly: no spawn, no I/O.
 */

const baseStorage = (overrides: Partial<IStorage> = {}): IStorage =>
  ({
    _id: BigInt(42),
    name: 'store',
    clientId: 'CLIENT_ID',
    clientSecret: 'CLIENT_SECRET',
    refreshToken: 'REFRESH',
    accessToken: 'ACCESS',
    expiry: new Date('2030-01-02T03:04:05.000Z'),
    folderId: 'FOLDER',
    kind: 3,
    folderName: '',
    publicUrl: '',
    secondPublicUrl: '',
    inStorage: '',
    used: 0,
    files: [],
    ...overrides
  } as IStorage & { _id: bigint });

describe('rcloneHelper.createRcloneConfig (characterization)', () => {
  // The OAuth token blob is a JSON string of access/refresh/type/expiry. expiry is
  // Luxon's ISO of the JS Date (offset form, e.g. ...+00:00). Derive it the same way
  // the code does so the assertion is stable regardless of the host timezone.
  const expiryDate = new Date('2030-01-02T03:04:05.000Z');
  const expectedTokenJson = JSON.stringify({
    access_token: 'ACCESS',
    token_type: 'Bearer',
    refresh_token: 'REFRESH',
    expiry: DateTime.fromJSDate(expiryDate).toISO()
  });

  describe('Google Drive (kind = 3)', () => {
    it('emits a drive remote with client creds, token and root_folder_id', () => {
      const cfg = rcloneHelper.createRcloneConfig(baseStorage({ kind: 3, folderId: 'ROOT_FOLDER' }));
      expect(cfg).toBe(
        `[42]\n` +
          `type = drive\n` +
          `client_id = CLIENT_ID\n` +
          `client_secret = CLIENT_SECRET\n` +
          `token = ${expectedTokenJson}\n` +
          `root_folder_id = ROOT_FOLDER\n\n`
      );
    });
  });

  describe('OneDrive personal (kind = other, e.g. 1)', () => {
    it('emits a onedrive remote with no root_folder_id / drive_id block', () => {
      const cfg = rcloneHelper.createRcloneConfig(baseStorage({ kind: 1 }));
      expect(cfg).toBe(
        `[42]\n` +
          `type = onedrive\n` +
          `client_id = CLIENT_ID\n` +
          `client_secret = CLIENT_SECRET\n` +
          `token = ${expectedTokenJson}\n`
      );
    });
  });

  describe('OneDrive business (kind = 6)', () => {
    it('parses folderId as "driveId#folderId" and emits the business drive block', () => {
      const cfg = rcloneHelper.createRcloneConfig(baseStorage({ kind: 6, folderId: 'DRIVE_ID#SUB_FOLDER' }));
      expect(cfg).toBe(
        `[42]\n` +
          `type = onedrive\n` +
          `client_id = CLIENT_ID\n` +
          `client_secret = CLIENT_SECRET\n` +
          `token = ${expectedTokenJson}\n` +
          `root_folder_id = DRIVE_ID#SUB_FOLDER\n` +
          `drive_id = DRIVE_ID\n` +
          `drive_type = business\n` +
          `no_versions = true\n\n`
      );
    });

    it('omits root_folder_id when folderId has no "#folder" segment', () => {
      const cfg = rcloneHelper.createRcloneConfig(baseStorage({ kind: 6, folderId: 'DRIVE_ID' }));
      expect(cfg).not.toContain('root_folder_id');
      expect(cfg).toContain('drive_id = DRIVE_ID\n');
      expect(cfg).toContain('drive_type = business\n');
    });
  });

  describe('S3-compatible (kind = 7 and kind = 8)', () => {
    it('emits a base s3 remote plus an alias remote pointing at bucket+prefix', () => {
      const cfg = rcloneHelper.createRcloneConfig(
        baseStorage({ kind: 7, folderId: 'PREFIX', publicUrl: 'https://s3.example.com/my-bucket' })
      );
      expect(cfg).toBe(
        `[42_s3]\n` +
          `type = s3\n` +
          `provider = Other\n` +
          `access_key_id = CLIENT_ID\n` +
          `secret_access_key = CLIENT_SECRET\n` +
          `endpoint = https://s3.example.com\n` +
          `region = auto\n\n` +
          `[42]\n` +
          `type = alias\n` +
          `remote = 42_s3:my-bucket/PREFIX\n\n`
      );
    });

    it('routes kind 8 through the same S3 builder', () => {
      const cfg = rcloneHelper.createRcloneConfig(
        baseStorage({ kind: 8, folderId: '', publicUrl: 'https://s3.example.com/my-bucket' })
      );
      expect(cfg).toContain('[42_s3]\n');
      expect(cfg).toContain('type = s3\n');
      // No folderId -> alias remote ends at the bucket with no folder prefix.
      expect(cfg).toContain('remote = 42_s3:my-bucket\n\n');
    });

    it('handles a :service_path public URL (first path segment folds into the endpoint, next is the bucket)', () => {
      // :service_path is rewritten to "s3" in place, so the leading path segment
      // becomes part of the endpoint and the following segment is the bucket.
      const cfg = rcloneHelper.createRcloneConfig(
        baseStorage({ kind: 7, folderId: '', publicUrl: 'https://host.example.com/:service_path/my-bucket' })
      );
      expect(cfg).toContain('endpoint = https://host.example.com/s3/\n');
      expect(cfg).toContain('remote = 42_s3:my-bucket\n\n');
    });
  });
});
