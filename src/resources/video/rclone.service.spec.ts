import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { getModelToken } from '@nestjs/mongoose';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Job } from 'bullmq';

import { RcloneService } from './rclone.service';
import { fileHelper, StringCrypto } from '../../utils';
import { IVideoData } from './interfaces';

/**
 * Runtime characterization for RcloneService.getLinkedSourceUrl, moved here in
 * the Slice C extraction (previously inline in VideoService). Pins CURRENT
 * behavior at f887000: the linkedStorage-vs-storage findOne selection + its
 * { publicUrl, folderId } projection, the missing-storage flow through the
 * onStorageNotFound callback seam, the !publicUrl -> null branch, the
 * encodeURIComponent path join, and the :service_path / :path URL substitution.
 *
 * The 'externalstorage' model is injected (@InjectModel) and mocked here via its
 * model token; findOne(...).lean().exec() is programmed on that mock. The decrypt
 * path (decryptToken / CRYPTO_SECRET_KEY) is intentionally NOT exercised — that
 * stays the security-reviewer's verified lane.
 */

const makeJob = (data: Partial<IVideoData>): Job<IVideoData> => ({ data } as Job<IVideoData>);

// Builds a RcloneService backed by a mocked 'externalstorage' model. Returns both
// so tests can program findOne and assert its query shape on the injected handle.
const buildService = async () => {
  const externalStorageMock = { findOne: jest.fn() };
  const module: TestingModule = await Test.createTestingModule({
    providers: [
      RcloneService,
      {
        provide: WINSTON_MODULE_PROVIDER,
        useValue: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn(), notice: jest.fn() }
      },
      { provide: ConfigService, useValue: { get: jest.fn() } },
      { provide: getModelToken('externalstorage'), useValue: externalStorageMock }
    ]
  }).compile();
  return { service: module.get<RcloneService>(RcloneService), externalStorageMock };
};

describe('RcloneService.getLinkedSourceUrl (characterization)', () => {
  let service: RcloneService;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let externalStorageMock: { findOne: jest.Mock };
  let findOneSpy: jest.Mock;

  // findOne(...).lean().exec() returns `doc`. Programs the injected model mock.
  const mockFindOne = (doc: unknown) => {
    externalStorageMock.findOne.mockReturnValue({ lean: () => ({ exec: () => Promise.resolve(doc) }) });
    return externalStorageMock.findOne;
  };

  beforeEach(async () => {
    ({ service, externalStorageMock } = await buildService());
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
  let externalStorageMock: { findOne: jest.Mock };

  const mockFindOne = (doc: unknown) => {
    externalStorageMock.findOne.mockReturnValue({ lean: () => ({ exec: () => Promise.resolve(doc) }) });
    return externalStorageMock.findOne;
  };

  beforeEach(async () => {
    ({ service, externalStorageMock } = await buildService());
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

/**
 * Characterization for the credential-decrypt tail of ensureRcloneConfigExist —
 * the path that decryptToken (rclone.service.ts:80-84) feeds. Unlike the
 * query-shape suites above this drives the SUCCESS branch to completion so the
 * decrypted secret actually reaches the on-disk config text.
 *
 * Two contracts are pinned:
 *  1. The plaintext that lands in the rclone config file is the real decrypted
 *     secret (a genuine StringCrypto round-trip, not a stub) — surgeon's
 *     local-copy hardening must NOT change what rclone receives.
 *  2. The INPUT storage object's clientSecret stays ENCRYPTED after the call.
 *     This locks the current in-place-mutation bug: today decryptToken writes
 *     the plaintext back onto the shared findOne().lean() object, so this
 *     assertion is EXPECTED-RED on unchanged code and must go green once the
 *     surgeon decrypts into a local copy. See _workspace/02_test_baseline.md.
 */
describe('RcloneService.ensureRcloneConfigExist credential decrypt (characterization)', () => {
  const CRYPTO_KEY = 'test-crypto-secret-key';

  // Mirrors the production StringCrypto so the test produces ciphertext decryptToken
  // can actually decrypt (same aes256 + sha256-derived key, IV appended after '.').
  const encryptSecret = async (plaintext: string) => {
    const stringCrypto = new StringCrypto(CRYPTO_KEY);
    return (await stringCrypto.encrypt(plaintext)) as string;
  };

  // Builds a RcloneService whose ConfigService returns a REAL crypto key, so the
  // decryptToken round-trip runs end to end. Returns the mocked model handle too.
  const buildCryptoService = async () => {
    const externalStorageMock = { findOne: jest.fn() };
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        RcloneService,
        {
          provide: WINSTON_MODULE_PROVIDER,
          useValue: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn(), notice: jest.fn() }
        },
        { provide: ConfigService, useValue: { get: jest.fn().mockReturnValue(CRYPTO_KEY) } },
        { provide: getModelToken('externalstorage'), useValue: externalStorageMock }
      ]
    }).compile();
    return { service: module.get<RcloneService>(RcloneService), externalStorageMock };
  };

  // Programs findOne(...).lean().exec() to return the given storage record.
  const mockFindOne = (externalStorageMock: { findOne: jest.Mock }, doc: unknown) => {
    externalStorageMock.findOne.mockReturnValue({ lean: () => ({ exec: () => Promise.resolve(doc) }) });
  };

  // A drive (kind 3) storage record whose clientSecret is genuinely encrypted.
  // kind 3 routes createRcloneConfig down the non-S3 branch which emits
  // `client_secret = <secret>` into the config text.
  const buildEncryptedStorage = async (encryptedSecret: string) => ({
    _id: BigInt(888),
    name: 'remote-888',
    clientId: 'client-id-888',
    clientSecret: encryptedSecret,
    refreshToken: 'refresh-token',
    accessToken: 'access-token',
    expiry: new Date('2030-01-01T00:00:00.000Z'),
    folderId: 'folder-888',
    kind: 3,
    folderName: 'f',
    publicUrl: '',
    secondPublicUrl: '',
    inStorage: '',
    used: 0,
    files: []
  });

  afterEach(() => jest.restoreAllMocks());

  it('writes the DECRYPTED plaintext secret into the rclone config file', async () => {
    const PLAINTEXT = 'super-secret-value';
    const encryptedSecret = await encryptSecret(PLAINTEXT);

    const { service, externalStorageMock } = await buildCryptoService();
    const storage = await buildEncryptedStorage(encryptedSecret);
    mockFindOne(externalStorageMock, storage);
    jest.spyOn(fileHelper, 'findInFile').mockResolvedValue(false); // config missing -> generate
    const appendSpy = jest.spyOn(fileHelper, 'appendToFile').mockResolvedValue(undefined as never);

    await service.ensureRcloneConfigExist('/config/rclone.conf', '888', makeJob({ storage: '888' }), jest.fn());

    // The config text appended to disk carries the real decrypted secret, never the ciphertext.
    expect(appendSpy).toHaveBeenCalledTimes(1);
    const configText = appendSpy.mock.calls[0][1] as string;
    expect(configText).toContain(`client_secret = ${PLAINTEXT}`);
    expect(configText).not.toContain(encryptedSecret);
  });

  it('leaves the INPUT storage object clientSecret ENCRYPTED after the call (no in-place mutation)', async () => {
    // EXPECTED-RED on current code: decryptToken mutates storage.clientSecret in place.
    // Goes GREEN after surgeon decrypts into a local copy. See 02_test_baseline.md.
    const PLAINTEXT = 'super-secret-value';
    const encryptedSecret = await encryptSecret(PLAINTEXT);

    const { service, externalStorageMock } = await buildCryptoService();
    const storage = await buildEncryptedStorage(encryptedSecret);
    mockFindOne(externalStorageMock, storage);
    jest.spyOn(fileHelper, 'findInFile').mockResolvedValue(false);
    jest.spyOn(fileHelper, 'appendToFile').mockResolvedValue(undefined as never);

    await service.ensureRcloneConfigExist('/config/rclone.conf', '888', makeJob({ storage: '888' }), jest.fn());

    expect(storage.clientSecret).toBe(encryptedSecret);
    expect(storage.clientSecret).not.toBe(PLAINTEXT);
  });
});
