import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { getQueueToken } from '@nestjs/bullmq';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import mongoose from 'mongoose';
import * as path from 'path';

import { VideoService } from './video.service';
import { EncodingArgsService } from './encoding-args.service';
import { QualityResolverService } from './quality-resolver.service';
import { ProcessSpawnerService } from './process-spawner.service';
import { CodecPresetRegistry } from './codec-preset.registry';
import { RcloneService } from './rclone.service';
import { TaskQueue, VideoCodec } from '../../enums';
import { DaplexApiService } from '../../common/modules/daplex-api';
import { TranscoderApiService } from '../../common/modules/transcoder-api';
import { rcloneHelper } from '../../utils';
import { mediaStorageModel } from '../../models/media-storage.model';

/**
 * Characterization tests for the quality-resolution helpers: calculateQuality,
 * findAvailableQuality, findExistingManifest.
 *
 * These pin the EXACT current behavior (concrete return values, not snapshots),
 * recorded GREEN against the methods while they still live on VideoService, then
 * `target` is repointed to QualityResolverService after the move. The describe/it
 * names and assertions are unchanged, proving the extracted logic is identical.
 */
describe('QualityResolverService (characterization)', () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let target: any;

  const configValues: Record<string, string | undefined> = {
    DATABASE_URL: 'mongodb://test/db',
    RCLONE_CONFIG_FILE: '/config/rclone.conf',
    RCLONE_DIR: '/opt/rclone'
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

    target = module.get<QualityResolverService>(QualityResolverService);
  });

  afterEach(() => jest.restoreAllMocks());

  // ---------------------------------------------------------------------------
  // calculateQuality (pure)
  // ---------------------------------------------------------------------------
  describe('calculateQuality', () => {
    const call = (h: number, q: number[], forced: number[] = [], fallback: number[] = []): number[] =>
      target.calculateQuality(h, q, forced, fallback);

    it('keeps qualities at or below the source height', () => {
      expect(call(1080, [2160, 1440, 1080, 720, 480])).toEqual([1080, 720, 480]);
    });
    it('includes forced qualities above the source height', () => {
      expect(call(1080, [2160, 1440, 1080, 720, 480], [2160])).toEqual([2160, 1080, 720, 480]);
    });
    it('returns empty when height is falsy', () => {
      expect(call(0, [1080, 720])).toEqual([]);
    });
    it('falls back when no quality is suitable', () => {
      expect(call(100, [480, 720], [], [480])).toEqual([480]);
    });
  });

  // ---------------------------------------------------------------------------
  // findAvailableQuality (mongo + media-storage stream cross-reference)
  // ---------------------------------------------------------------------------
  describe('findAvailableQuality', () => {
    const parsedInput = path.parse('/transcode/source.mkv'); // name = 'source'
    const job = { data: { _id: '999' } } as any;

    // Returns the findOne spy so the query shape can be asserted. The filter
    // ({ _id: BigInt(job.data._id) }) with NO projection is the migration-invariant
    // contract: it must survive the singleton -> @InjectModel switch byte-identically.
    // connect/disconnect are stubbed only so the current per-job lifecycle doesn't
    // hit a real DB; their call-shape is pinned separately in the REPOINT block.
    const mockStreams = (streams: Array<{ codec: number; _id: bigint; quality: number }>) => {
      jest.spyOn(mongoose, 'connect').mockResolvedValue(undefined as any);
      jest.spyOn(mongoose, 'disconnect').mockResolvedValue(undefined as any);
      return jest
        .spyOn(mediaStorageModel, 'findOne')
        .mockReturnValue({ lean: () => ({ exec: () => Promise.resolve({ streams }) }) } as any);
    };

    it('returns qualities whose encoded streams are not yet present', async () => {
      const findOneSpy = mockStreams([
        { codec: VideoCodec.H264, _id: BigInt(123), quality: 1080 },
        { codec: VideoCodec.H264, _id: BigInt(124), quality: 720 }
      ]);
      const result = await target.findAvailableQuality(
        ['123/source_1080.mp4', '124/source_720.mp4', 'abc/source_480.mp4'],
        [1080, 720, 480],
        parsedInput,
        VideoCodec.H264,
        [],
        job
      );
      // 123/1080 & 124/720 already encoded for H264; abc skipped (NaN id). 480 has no encoded stream.
      expect(result).toEqual([480]);
      // Query-shape contract (must survive @InjectModel migration): the source
      // doc is fetched by BigInt id with NO projection (single-arg findOne).
      expect(findOneSpy).toHaveBeenCalledTimes(1);
      expect(findOneSpy).toHaveBeenCalledWith({ _id: BigInt('999') });
      expect(findOneSpy.mock.calls[0]).toHaveLength(1);
    });

    it('treats replaceStreams ids as not-yet-encoded', async () => {
      mockStreams([
        { codec: VideoCodec.H264, _id: BigInt(123), quality: 1080 },
        { codec: VideoCodec.H264, _id: BigInt(124), quality: 720 }
      ]);
      const result = await target.findAvailableQuality(
        ['123/source_1080.mp4', '124/source_720.mp4'],
        [1080, 720, 480],
        parsedInput,
        VideoCodec.H264,
        ['123'],
        job
      );
      // 123 excluded from fileIds -> only 720 counts as encoded -> 1080 & 480 available.
      expect(result).toEqual([1080, 480]);
    });

    it('ignores streams of a different codec', async () => {
      mockStreams([{ codec: VideoCodec.VP9, _id: BigInt(123), quality: 1080 }]);
      const result = await target.findAvailableQuality(
        ['123/source_1080.mp4'],
        [1080, 720],
        parsedInput,
        VideoCodec.H264,
        [],
        job
      );
      expect(result).toEqual([1080, 720]);
    });

    // -------------------------------------------------------------------------
    // REPOINT-AFTER-MIGRATION: per-job connection lifecycle.
    //
    // These assertions pin the CURRENT per-job connect/disconnect that brackets
    // the findOne. The persistent-Mongoose migration REMOVES this lifecycle (the
    // connection moves to MongooseModule.forRootAsync), so the surgeon's repoint
    // is: DELETE this block and add the post-migration invariant (connect/disconnect
    // are NEVER called by the service). Do NOT delete the query-shape assertions
    // above — those survive the migration unchanged. See 02_test_baseline.md.
    // -------------------------------------------------------------------------
    it('[repoint] opens a per-job connection with family:4 + useBigInt64 and disconnects after the query', async () => {
      const connectSpy = jest.spyOn(mongoose, 'connect').mockResolvedValue(undefined as any);
      const disconnectSpy = jest.spyOn(mongoose, 'disconnect').mockResolvedValue(undefined as any);
      jest
        .spyOn(mediaStorageModel, 'findOne')
        .mockReturnValue({ lean: () => ({ exec: () => Promise.resolve({ streams: [] }) }) } as any);

      await target.findAvailableQuality(['123/source_1080.mp4'], [1080], parsedInput, VideoCodec.H264, [], job);

      expect(connectSpy).toHaveBeenCalledTimes(1);
      expect(connectSpy).toHaveBeenCalledWith('mongodb://test/db', { family: 4, useBigInt64: true });
      expect(disconnectSpy).toHaveBeenCalledTimes(1);
    });
  });

  // ---------------------------------------------------------------------------
  // findExistingManifest (rclone remote read)
  // ---------------------------------------------------------------------------
  describe('findExistingManifest', () => {
    it('returns null when the remote folder does not exist', async () => {
      const isPathExist = jest.spyOn(rcloneHelper, 'isPathExist').mockResolvedValue(false);
      const listRemoteJson = jest.spyOn(rcloneHelper, 'listRemoteJson');
      const result = await target.findExistingManifest('gd', '42', VideoCodec.H264);
      expect(result).toBeNull();
      expect(isPathExist).toHaveBeenCalled();
      expect(listRemoteJson).not.toHaveBeenCalled();
    });

    it('returns null when no manifest file is found', async () => {
      jest.spyOn(rcloneHelper, 'isPathExist').mockResolvedValue(true);
      jest.spyOn(rcloneHelper, 'listRemoteJson').mockResolvedValue([]);
      const readRemoteFile = jest.spyOn(rcloneHelper, 'readRemoteFile');
      const result = await target.findExistingManifest('gd', '42', VideoCodec.H264);
      expect(result).toBeNull();
      expect(readRemoteFile).not.toHaveBeenCalled();
    });

    it('returns null when the manifest file is empty', async () => {
      jest.spyOn(rcloneHelper, 'isPathExist').mockResolvedValue(true);
      jest.spyOn(rcloneHelper, 'listRemoteJson').mockResolvedValue([{ Path: 'sub/manifest_1.json' }] as any);
      jest.spyOn(rcloneHelper, 'readRemoteFile').mockResolvedValue('');
      const result = await target.findExistingManifest('gd', '42', VideoCodec.H264);
      expect(result).toBeNull();
    });

    it('parses and returns the manifest when present', async () => {
      const manifest = { videoTracks: [{ uri: '1/source_1080.mp4' }], audioTracks: [] };
      jest.spyOn(rcloneHelper, 'isPathExist').mockResolvedValue(true);
      jest.spyOn(rcloneHelper, 'listRemoteJson').mockResolvedValue([{ Path: 'sub/manifest_1.json' }] as any);
      jest.spyOn(rcloneHelper, 'readRemoteFile').mockResolvedValue(JSON.stringify(manifest));
      const result = await target.findExistingManifest('gd', '42', VideoCodec.H264);
      expect(result).toEqual(manifest);
    });
  });
});
