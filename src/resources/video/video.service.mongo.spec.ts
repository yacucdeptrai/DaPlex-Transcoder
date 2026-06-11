import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { getQueueToken } from '@nestjs/bullmq';
import { getModelToken } from '@nestjs/mongoose';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import mongoose from 'mongoose';

import { VideoService } from './video.service';
import { EncodingArgsService } from './encoding-args.service';
import { QualityResolverService } from './quality-resolver.service';
import { ProcessSpawnerService } from './process-spawner.service';
import { CodecPresetRegistry } from './codec-preset.registry';
import { RcloneService } from './rclone.service';
import { TaskQueue, VideoCodec } from '../../enums';
import { DaplexApiService } from '../../common/modules/daplex-api';
import { TranscoderApiService } from '../../common/modules/transcoder-api';
import { fileHelper } from '../../utils';

/**
 * Characterization tests for the Mongo reads VideoService.transcode() performs
 * before any encoding work (Phase 7.6 persistent-Mongoose migration target).
 *
 * The MIGRATION-INVARIANT contract these lock is the per-document query SHAPE —
 * each findOne's filter ({}, { _id: BigInt(...) }) and projection — and the
 * .lean().exec() chain. After the singleton -> @InjectModel('<name>') switch the
 * reads run on the four injected models, which are mocked here via their model
 * tokens; the asserted filter/projection shapes are unchanged from the baseline.
 *
 * The connection is owned by MongooseModule, so the service must NOT open/close
 * its own — pinned in the lifecycle test below.
 *
 * No real DB connection is ever opened (the model mocks are pure), and transcode()
 * is intentionally short-circuited right after the reads by making
 * fileHelper.createDir throw a sentinel, so no rclone/ffmpeg runs.
 */
describe('VideoService.transcode Mongo reads (characterization)', () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let service: VideoService;

  const configValues: Record<string, string | undefined> = {
    DATABASE_URL: 'mongodb://test/db',
    RCLONE_DIR: '/opt/rclone',
    RCLONE_CONFIG_FILE: '/config/rclone.conf',
    TRANSCODE_DIR: '/transcode',
    FFMPEG_DIR: '/opt/ffmpeg',
    MEDIAINFO_DIR: '/opt/mediainfo'
    // USE_URL_INPUT unset -> UseURLInput=false, so getLinkedSourceUrl is skipped.
  };

  const makeJob = () =>
    ({
      id: 'job-1',
      data: {
        _id: '555',
        media: '111',
        storage: '222',
        filename: 'source.mkv',
        path: 'movies',
        codec: VideoCodec.H264
      }
    } as any);

  // Builds a findOne return value matching the real chain: findOne(...).lean().exec().
  const leanExec = (doc: unknown) => ({ lean: () => ({ exec: () => Promise.resolve(doc) }) });

  // Sentinel thrown by createDir to halt transcode() cleanly right after the
  // Mongo read window (everything past it is rclone/ffmpeg, out of scope here).
  const HALT = new Error('__halt_after_reads__');

  let connectSpy: jest.SpyInstance;
  let disconnectSpy: jest.SpyInstance;
  let settingFindOne: jest.Mock;
  let mediaFindOne: jest.Mock;
  let externalFindOne: jest.Mock;
  let mediaStorageFindOne: jest.Mock;

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
        { provide: TranscoderApiService, useValue: {} },
        { provide: getModelToken('setting'), useValue: { findOne: jest.fn() } },
        { provide: getModelToken('media'), useValue: { findOne: jest.fn() } },
        { provide: getModelToken('externalstorage'), useValue: { findOne: jest.fn() } },
        { provide: getModelToken('mediastorage'), useValue: { findOne: jest.fn() } }
      ]
    }).compile();

    service = module.get<VideoService>(VideoService);

    // The migration owns the connection in MongooseModule — the service no longer
    // calls these. Spied (not stubbed) so the lifecycle test can assert never-called.
    connectSpy = jest.spyOn(mongoose, 'connect');
    disconnectSpy = jest.spyOn(mongoose, 'disconnect');

    // Program each injected model. mediaStorage (the source) returns a falsy quality
    // so validateSourceQuality is skipped and execution proceeds to createDir
    // without needing the encoding pipeline.
    settingFindOne = module.get(getModelToken('setting')).findOne;
    mediaFindOne = module.get(getModelToken('media')).findOne;
    externalFindOne = module.get(getModelToken('externalstorage')).findOne;
    mediaStorageFindOne = module.get(getModelToken('mediastorage')).findOne;
    settingFindOne.mockReturnValue(leanExec({}));
    mediaFindOne.mockReturnValue(leanExec({ originalLang: 'en' }));
    externalFindOne.mockReturnValue(leanExec({ publicUrl: 'https://cdn/' }));
    mediaStorageFindOne.mockReturnValue(leanExec({ name: 'src', quality: 0 }));

    // rclone config check is exercised inside transcode() before the source read.
    jest.spyOn(RcloneService.prototype, 'ensureRcloneConfigExist').mockResolvedValue(undefined as any);
    // No leftover transcode dir, so the retry-cleanup branch is skipped.
    jest.spyOn(fileHelper, 'fileExists').mockResolvedValue(false);
    jest.spyOn(fileHelper, 'deleteFolder').mockResolvedValue(undefined as any);
    // Halt right after the disconnect — past this is rclone/ffmpeg, out of scope.
    jest.spyOn(fileHelper, 'createDir').mockRejectedValue(HALT);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  const runToReadWindow = async () => {
    await expect(service.transcode(makeJob(), VideoCodec.H264)).rejects.toBe(HALT);
  };

  it('reads app settings with an empty filter and no projection', async () => {
    await runToReadWindow();
    expect(settingFindOne).toHaveBeenCalledTimes(1);
    expect(settingFindOne).toHaveBeenCalledWith({});
    expect(settingFindOne.mock.calls[0]).toHaveLength(1); // no projection arg
  });

  it('reads media by BigInt id projecting only _id + originalLang', async () => {
    await runToReadWindow();
    expect(mediaFindOne).toHaveBeenCalledTimes(1);
    expect(mediaFindOne).toHaveBeenCalledWith({ _id: BigInt('111') }, { _id: 1, originalLang: 1 });
  });

  it('reads the external storage by BigInt id projecting only _id + publicUrl', async () => {
    await runToReadWindow();
    expect(externalFindOne).toHaveBeenCalledTimes(1);
    expect(externalFindOne).toHaveBeenCalledWith({ _id: BigInt('222') }, { _id: 1, publicUrl: 1 });
  });

  it('reads the source media-storage by BigInt id projecting only _id + name + quality', async () => {
    await runToReadWindow();
    expect(mediaStorageFindOne).toHaveBeenCalledTimes(1);
    expect(mediaStorageFindOne).toHaveBeenCalledWith({ _id: BigInt('555') }, { _id: 1, name: 1, quality: 1 });
  });

  // ---------------------------------------------------------------------------
  // Post-migration lifecycle invariant: the connection is owned by MongooseModule
  // (forRootAsync), so transcode() must NOT open/close its own mongoose connection.
  // Replaces the pre-migration per-job connect/disconnect assertion.
  // ---------------------------------------------------------------------------
  it('does not open or close its own mongoose connection (owned by MongooseModule)', async () => {
    await runToReadWindow();
    expect(connectSpy).not.toHaveBeenCalled();
    expect(disconnectSpy).not.toHaveBeenCalled();
  });
});
