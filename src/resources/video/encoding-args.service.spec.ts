import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { getQueueToken } from '@nestjs/bullmq';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import * as path from 'path';

import { VideoService } from './video.service';
import { AudioCodec, TaskQueue, VideoCodec } from '../../enums';
import { DaplexApiService } from '../../common/modules/daplex-api';
import { TranscoderApiService } from '../../common/modules/transcoder-api';
import { EncodingArgsService } from './encoding-args.service';
import { QualityResolverService } from './quality-resolver.service';

/**
 * Characterization tests for the ffmpeg/mp4box/rclone argument builders.
 *
 * These pin the EXACT arg arrays produced today for every meaningful branch of
 * the 7 public builder methods, so hoisting them out of VideoService into a
 * dedicated EncodingArgsService is provably behavior-preserving.
 *
 * NOTE: `target` is the object under test. It is wired to VideoService while the
 * methods still live there (capture run), then repointed to EncodingArgsService
 * after the extraction. The describe/it names — and therefore the recorded
 * snapshots — stay identical across both runs, which is exactly what proves the
 * extracted output is byte-for-byte unchanged.
 */
describe('EncodingArgsService (characterization)', () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let target: any;

  const configValues: Record<string, string | undefined> = {
    RCLONE_CONFIG_FILE: '/config/rclone.conf'
    // USE_URL_INPUT, SVT_AV1_PRESET, * → undefined (matches production defaults)
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        VideoService,
        EncodingArgsService,
        QualityResolverService,
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

    target = module.get<EncodingArgsService>(EncodingArgsService);
  });

  // ---------------------------------------------------------------------------
  // createAudioEncodingArgs
  // ---------------------------------------------------------------------------
  describe('createAudioEncodingArgs', () => {
    const parsedInput = path.parse('/transcode/42/source.mkv');
    const baseAudio = {
      inputFile: '/transcode/42/source.mkv',
      parsedInput,
      audioParams: ['-c:a', 'libfdk_aac'],
      codec: AudioCodec.AAC,
      channels: 2,
      downmix: false,
      audioIndex: 1,
      outputFileName: 'audio_aac.mp4'
    };
    const call = (overrides: Record<string, unknown> = {}): string[] =>
      target.createAudioEncodingArgs({ ...baseAudio, ...overrides });

    it('AAC stereo', () => expect(call()).toMatchSnapshot());
    it('AAC 5.1 downmix to stereo', () => expect(call({ channels: 6, downmix: true })).toMatchSnapshot());
    it('AAC 5.1 multichannel (no downmix)', () => expect(call({ channels: 6, downmix: false })).toMatchSnapshot());
    it('AAC channels above MAX clamp', () => expect(call({ channels: 10, downmix: false })).toMatchSnapshot());
    it('OPUS stereo (bitrate applied)', () =>
      expect(call({ codec: AudioCodec.OPUS, channels: 2, audioParams: ['-c:a', 'libopus'] })).toMatchSnapshot());
    it('OPUS downmix to stereo', () =>
      expect(
        call({ codec: AudioCodec.OPUS, channels: 6, downmix: true, audioParams: ['-c:a', 'libopus'] })
      ).toMatchSnapshot());
    it('OPUS_SURROUND multichannel (per-channel bitrate + mapping_family 1)', () =>
      expect(
        call({ codec: AudioCodec.OPUS_SURROUND, channels: 6, downmix: false, audioParams: ['-c:a', 'libopus'] })
      ).toMatchSnapshot());
    it('URL input adds reconnect flags', () => {
      target.UseURLInput = true;
      expect(call()).toMatchSnapshot();
    });
  });

  // ---------------------------------------------------------------------------
  // createVideoEncodingArgs (single pass)
  // ---------------------------------------------------------------------------
  describe('createVideoEncodingArgs', () => {
    const parsedInput = path.parse('/transcode/42/source.mkv');
    const sourceInfo = {
      codec: 'h264',
      duration: 120,
      fps: 24,
      bitrate: 5000,
      width: 1920,
      height: 1080,
      language: null,
      isHDR: false,
      sourceH264Params: '',
      hdrParams: null
    } as any;
    const advancedSettings = { h264Tune: '' } as any;
    const encodingSetting = {
      crf: 23,
      h265Crf: 28,
      cq: 30,
      useLowerRate: false,
      maxrate: 8000,
      bufsize: 16000
    } as any;
    const baseVideo = {
      inputFile: '/transcode/42/source.mkv',
      parsedInput,
      codec: VideoCodec.VP9,
      quality: 1080,
      videoParams: ['-c:v', 'libvpx-vp9'],
      sourceInfo,
      crfKey: 'cq' as const,
      advancedSettings,
      encodingSetting,
      outputFileName: 'v_1080.webm'
    };
    const call = (overrides: Record<string, unknown> = {}): string[] =>
      target.createVideoEncodingArgs({ ...baseVideo, ...overrides });

    it('VP9 with encoding setting', () => expect(call()).toMatchSnapshot());
    it('VP9 without encoding setting', () => expect(call({ encodingSetting: undefined })).toMatchSnapshot());
    it('VP9 useLowerRate (baseBitrate below maxrate)', () =>
      expect(call({ encodingSetting: { ...encodingSetting, useLowerRate: true } })).toMatchSnapshot());
    it('H264 with encoding setting (crf)', () =>
      expect(call({ codec: VideoCodec.H264, crfKey: 'crf' })).toMatchSnapshot());
    it('H264 HDR tonemap', () =>
      expect(
        call({ codec: VideoCodec.H264, crfKey: 'crf', sourceInfo: { ...sourceInfo, isHDR: true } })
      ).toMatchSnapshot());
    it('H264 >=1440 with source x264 params (profile level)', () =>
      expect(
        call({
          codec: VideoCodec.H264,
          crfKey: 'crf',
          quality: 2160,
          sourceInfo: { ...sourceInfo, width: 3840, height: 2160, sourceH264Params: 'ref=4:deblock=-1,-1' }
        })
      ).toMatchSnapshot());
    it('AV1 main preset (no HDR params)', () =>
      expect(call({ codec: VideoCodec.AV1, crfKey: 'cq' })).toMatchSnapshot());
    it('AV1 with HDR params', () =>
      expect(
        call({
          codec: VideoCodec.AV1,
          crfKey: 'cq',
          sourceInfo: {
            ...sourceInfo,
            hdrParams: { ffmpegParams: ['-color_primaries', 'bt2020'], libsvtav1Params: 'mastering-display=foo' }
          }
        })
      ).toMatchSnapshot());
    it('AV1 animation tune (skips scm=0)', () =>
      expect(
        call({ codec: VideoCodec.AV1, crfKey: 'cq', advancedSettings: { h264Tune: 'animation' } })
      ).toMatchSnapshot());
    it('split-from / split-duration', () => expect(call({ splitFrom: '10.5', splitDuration: '30' })).toMatchSnapshot());
    it('URL input adds reconnect flags', () => {
      target.UseURLInput = true;
      expect(call()).toMatchSnapshot();
    });
  });

  // ---------------------------------------------------------------------------
  // createConcatSegmentArgs
  // ---------------------------------------------------------------------------
  describe('createConcatSegmentArgs', () => {
    const parsedInput = path.parse('/transcode/42/source.mkv');
    it('pinned args', () =>
      expect(
        target.createConcatSegmentArgs('/transcode/42/concat.txt', parsedInput, 'out_1080.mp4')
      ).toMatchSnapshot());
    it('copies the stream and writes under parsedInput.dir', () => {
      const args = target.createConcatSegmentArgs('/transcode/42/concat.txt', parsedInput, 'out_1080.mp4');
      expect(args).toContain('concat');
      expect(args.slice(-3)).toEqual(['-c', 'copy', `"${parsedInput.dir}/out_1080.mp4"`]);
    });
  });

  // ---------------------------------------------------------------------------
  // createMP4BoxPackArgs
  // ---------------------------------------------------------------------------
  describe('createMP4BoxPackArgs', () => {
    const parsedInput = path.parse('/transcode/42/source.mkv');
    it('pinned args', () =>
      expect(
        target.createMP4BoxPackArgs('/transcode/42/v_1080.mp4', parsedInput, 'v_1080_', 'v_1080.mpd')
      ).toMatchSnapshot());
  });

  // ---------------------------------------------------------------------------
  // createRcloneMoveArgs
  // ---------------------------------------------------------------------------
  describe('createRcloneMoveArgs', () => {
    it('without include filter', () =>
      expect(target.createRcloneMoveArgs('/transcode/42/out', 'gd:media/42')).toMatchSnapshot());
    it('with include filter', () =>
      expect(target.createRcloneMoveArgs('/transcode/42/out', 'gd:media/42', '*.mp4')).toMatchSnapshot());
    it('uses the configured rclone config file', () => {
      const args = target.createRcloneMoveArgs('/src', 'dst');
      expect(args[0]).toBe('--config');
      expect(args[1]).toBe('/config/rclone.conf');
    });
  });

  // ---------------------------------------------------------------------------
  // createRcloneMoveThumbArgs
  // ---------------------------------------------------------------------------
  describe('createRcloneMoveThumbArgs', () => {
    it('move (default)', () =>
      expect(target.createRcloneMoveThumbArgs('/transcode/42', 'gd', 'media/42')).toMatchSnapshot());
    it('sync', () =>
      expect(target.createRcloneMoveThumbArgs('/transcode/42', 'gd', 'media/42', true)).toMatchSnapshot());
  });
});
