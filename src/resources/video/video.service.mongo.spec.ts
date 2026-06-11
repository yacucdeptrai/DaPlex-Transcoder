import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { getQueueToken } from '@nestjs/bullmq';
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
import { settingModel } from '../../models/setting.model';
import { mediaModel } from '../../models/media.model';
import { externalStorageModel } from '../../models/external-storage.model';
import { mediaStorageModel } from '../../models/media-storage.model';

/**
 * Characterization tests for the Mongo reads VideoService.transcode() performs
 * before any encoding work (Phase 7.6 persistent-Mongoose migration target).
 *
 * The MIGRATION-INVARIANT contract these lock is the per-document query SHAPE —
 * each findOne's filter ({}, { _id: BigInt(...) }) and projection — and the
 * .lean().exec() chain. The surgeon keeps every filter/projection byte-identical
 * when the singleton imports become @InjectModel('<name>')-injected Model<T>;
 * once that lands these assertions are repointed (see 02_test_baseline.md) to spy
 * on the injected model mocks, but the asserted shapes do NOT change.
 *
 * The per-job mongoose.connect/disconnect lifecycle is pinned only in the
 * [repoint] test below — it is the thing the migration REMOVES.
 *
 * No real DB connection is ever opened (mongoose.connect/disconnect are stubbed),
 * and transcode() is intentionally short-circuited right after the reads by
 * making fileHelper.createDir throw a sentinel, so no rclone/ffmpeg runs.
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
  let settingFindOne: jest.SpyInstance;
  let mediaFindOne: jest.SpyInstance;
  let externalFindOne: jest.SpyInstance;
  let mediaStorageFindOne: jest.SpyInstance;

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

    service = module.get<VideoService>(VideoService);

    connectSpy = jest.spyOn(mongoose, 'connect').mockResolvedValue(undefined as any);
    disconnectSpy = jest.spyOn(mongoose, 'disconnect').mockResolvedValue(undefined as any);

    // Each model read returns a minimal doc. mediaStorage (the source) returns a
    // falsy quality so validateSourceQuality is skipped and execution proceeds to
    // disconnect + createDir without needing the encoding pipeline.
    settingFindOne = jest.spyOn(settingModel, 'findOne').mockReturnValue(leanExec({}) as any);
    mediaFindOne = jest.spyOn(mediaModel, 'findOne').mockReturnValue(leanExec({ originalLang: 'en' }) as any);
    externalFindOne = jest
      .spyOn(externalStorageModel, 'findOne')
      .mockReturnValue(leanExec({ publicUrl: 'https://cdn/' }) as any);
    mediaStorageFindOne = jest
      .spyOn(mediaStorageModel, 'findOne')
      .mockReturnValue(leanExec({ name: 'src', quality: 0 }) as any);

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
  // REPOINT-AFTER-MIGRATION: per-job connection lifecycle.
  //
  // Pins the CURRENT connect-before / disconnect-after window around the reads.
  // The persistent-Mongoose migration REMOVES this (connection ownership moves to
  // MongooseModule.forRootAsync). Surgeon repoint: DELETE this test and replace it
  // with the post-migration invariant (mongoose.connect/disconnect are NEVER called
  // by the service). The query-shape tests above survive unchanged.
  // ---------------------------------------------------------------------------
  it('[repoint] opens one per-job connection (family:4 + useBigInt64) and disconnects after the reads', async () => {
    await runToReadWindow();
    expect(connectSpy).toHaveBeenCalledTimes(1);
    expect(connectSpy).toHaveBeenCalledWith('mongodb://test/db', { family: 4, useBigInt64: true });
    // disconnect is reached because the source quality is falsy (validateSourceQuality skipped).
    expect(disconnectSpy).toHaveBeenCalledTimes(1);
  });
});
