import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Job } from 'bullmq';

import { RcloneService } from './rclone.service';
import { externalStorageModel } from '../../models/external-storage.model';
import { fileHelper } from '../../utils';
import { IVideoData } from './interfaces';

/**
 * Runtime characterization for RcloneService.getLinkedSourceUrl, moved here in
 * the Slice C extraction (previously inline in VideoService). Pins CURRENT
 * behavior at f887000: the linkedStorage-vs-storage findOne selection + its
 * { publicUrl, folderId } projection, the missing-storage flow through the
 * onStorageNotFound callback seam, the !publicUrl -> null branch, the
 * encodeURIComponent path join, and the :service_path / :path URL substitution.
 *
 * The Mongoose model is mocked (findOne().lean().exec()). The decrypt path
 * (decryptToken / CRYPTO_SECRET_KEY) is intentionally NOT exercised here — that
 * stays the security-reviewer's verified lane.
 */

// Mongoose findOne(...).lean().exec() returns `doc`. Capture the call args so the
// projection + queried _id can be asserted.
function mockFindOne(doc: unknown) {
  return jest.spyOn(externalStorageModel, 'findOne').mockReturnValue({
    lean: () => ({ exec: () => Promise.resolve(doc) })
  } as any);
}

const makeJob = (data: Partial<IVideoData>): Job<IVideoData> => ({ data } as Job<IVideoData>);

describe('RcloneService.getLinkedSourceUrl (characterization)', () => {
  let service: RcloneService;
  let findOneSpy: jest.SpyInstance;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        RcloneService,
        {
          provide: WINSTON_MODULE_PROVIDER,
          useValue: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn(), notice: jest.fn() }
        },
        { provide: ConfigService, useValue: { get: jest.fn() } }
      ]
    }).compile();
    service = module.get<RcloneService>(RcloneService);
  });

  afterEach(() => jest.restoreAllMocks());

  // ---------------------------------------------------------------------------
  // storage selection: linkedStorage takes precedence over storage
  // ---------------------------------------------------------------------------
  it('queries linkedStorage (not storage) when job.data.linkedStorage is set, with the publicUrl/folderId projection', async () => {
    findOneSpy = mockFindOne({ publicUrl: 'https://cdn.example.com/:path', folderId: '' });
    const job = makeJob({ linkedStorage: '101', storage: '999', path: 'movies', filename: 'a.mkv' });

    await service.getLinkedSourceUrl(job, jest.fn());

    expect(findOneSpy).toHaveBeenCalledTimes(1);
    expect(findOneSpy).toHaveBeenCalledWith({ _id: BigInt(101) }, { publicUrl: 1, folderId: 1 });
  });

  it('queries storage when job.data.linkedStorage is absent', async () => {
    findOneSpy = mockFindOne({ publicUrl: 'https://cdn.example.com/:path', folderId: '' });
    const job = makeJob({ storage: '777', path: 'movies', filename: 'a.mkv' });

    await service.getLinkedSourceUrl(job, jest.fn());

    expect(findOneSpy).toHaveBeenCalledWith({ _id: BigInt(777) }, { publicUrl: 1, folderId: 1 });
  });

  // ---------------------------------------------------------------------------
  // missing-storage flow through the onStorageNotFound callback seam
  // ---------------------------------------------------------------------------
  it('invokes onStorageNotFound and throws its errorCode when the storage record is missing', async () => {
    mockFindOne(null);
    const onStorageNotFound = jest.fn().mockResolvedValue({ errorCode: 'STORAGE_NOT_FOUND' });
    const job = makeJob({ storage: '5', path: 'p', filename: 'f.mkv' });

    await expect(service.getLinkedSourceUrl(job, onStorageNotFound)).rejects.toThrow('STORAGE_NOT_FOUND');
    expect(onStorageNotFound).toHaveBeenCalledTimes(1);
    expect(onStorageNotFound).toHaveBeenCalledWith(job);
  });

  // ---------------------------------------------------------------------------
  // !publicUrl -> null
  // ---------------------------------------------------------------------------
  it('returns null when the storage record has no publicUrl', async () => {
    mockFindOne({ publicUrl: '', folderId: 'F' });
    const job = makeJob({ storage: '5', path: 'p', filename: 'f.mkv' });

    await expect(service.getLinkedSourceUrl(job, jest.fn())).resolves.toBeNull();
  });

  // ---------------------------------------------------------------------------
  // path build (encodeURIComponent + posix join) and :path substitution
  // ---------------------------------------------------------------------------
  it('substitutes :path with the encodeURIComponent-joined folderId/path/filename', async () => {
    mockFindOne({ publicUrl: 'https://cdn.example.com/bucket/:path', folderId: 'fold er' });
    const job = makeJob({ storage: '5', path: 'sub dir', filename: 'my file.mkv' });

    const url = await service.getLinkedSourceUrl(job, jest.fn());

    // each segment is encodeURIComponent'd then posix-joined: spaces -> %20.
    expect(url).toBe('https://cdn.example.com/bucket/fold%20er/sub%20dir/my%20file.mkv');
  });

  it('drops an empty folderId segment from the joined path', async () => {
    mockFindOne({ publicUrl: 'https://cdn.example.com/:path', folderId: '' });
    const job = makeJob({ storage: '5', path: 'dir', filename: 'f.mkv' });

    const url = await service.getLinkedSourceUrl(job, jest.fn());

    expect(url).toBe('https://cdn.example.com/dir/f.mkv');
  });

  // ---------------------------------------------------------------------------
  // :service_path -> s3 substitution (runs before :path)
  // ---------------------------------------------------------------------------
  it('substitutes :service_path with s3 as well as :path', async () => {
    mockFindOne({ publicUrl: 'https://host.example.com/:service_path/bucket/:path', folderId: '' });
    const job = makeJob({ storage: '5', path: 'd', filename: 'f.mkv' });

    const url = await service.getLinkedSourceUrl(job, jest.fn());

    expect(url).toBe('https://host.example.com/s3/bucket/d/f.mkv');
  });
});

/**
 * Characterization for RcloneService.ensureRcloneConfigExist — the SECOND
 * externalStorageModel query site in this service (rclone.service.ts:37-40),
 * distinct from getLinkedSourceUrl because it uses NO projection.
 *
 * This is a migrated call site (singleton -> @InjectModel('externalstorage')),
 * so the query shape must be locked. The decrypt/createRcloneConfig success tail
 * is intentionally not exercised (findOne returns null) — that keeps this a pure
 * query-shape + lifecycle net and leaves the crypto path in the reviewer's lane.
 */
describe('RcloneService.ensureRcloneConfigExist (characterization)', () => {
  let service: RcloneService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        RcloneService,
        {
          provide: WINSTON_MODULE_PROVIDER,
          useValue: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn(), notice: jest.fn() }
        },
        { provide: ConfigService, useValue: { get: jest.fn() } }
      ]
    }).compile();
    service = module.get<RcloneService>(RcloneService);
  });

  afterEach(() => jest.restoreAllMocks());

  it('queries externalStorage by BigInt id with NO projection when the rclone config is missing', async () => {
    jest.spyOn(fileHelper, 'findInFile').mockResolvedValue(false); // config not present -> query runs
    const findOneSpy = mockFindOne(null); // missing record -> stops at onStorageNotFound
    const onStorageNotFound = jest.fn().mockResolvedValue({ errorCode: 'STORAGE_NOT_FOUND' });
    const job = makeJob({ storage: '888' });

    await expect(service.ensureRcloneConfigExist('/config/rclone.conf', '888', job, onStorageNotFound)).rejects.toThrow(
      'STORAGE_NOT_FOUND'
    );

    // Query-shape contract (survives @InjectModel migration): single-arg findOne, no projection.
    expect(findOneSpy).toHaveBeenCalledTimes(1);
    expect(findOneSpy).toHaveBeenCalledWith({ _id: BigInt('888') });
    expect(findOneSpy.mock.calls[0]).toHaveLength(1);
    expect(onStorageNotFound).toHaveBeenCalledWith(job);
  });

  it('does NOT query externalStorage when the rclone config already exists', async () => {
    jest.spyOn(fileHelper, 'findInFile').mockResolvedValue(true); // config present -> short-circuit
    const findOneSpy = mockFindOne(null);
    const job = makeJob({ storage: '888' });

    await service.ensureRcloneConfigExist('/config/rclone.conf', '888', job, jest.fn());

    expect(findOneSpy).not.toHaveBeenCalled();
  });
});
