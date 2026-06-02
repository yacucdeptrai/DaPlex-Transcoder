import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { getQueueToken } from '@nestjs/bullmq';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import * as path from 'path';

import { VideoService } from './video.service';
import { TaskQueue, VideoCodec } from '../../enums';
import { DaplexApiService } from '../../common/modules/daplex-api';
import { TranscoderApiService } from '../../common/modules/transcoder-api';
import { EncodingArgsService } from './encoding-args.service';

/**
 * Characterization tests for createTwoPassesVideoEncodingArgs (Phase 6.11).
 *
 * The pass=1 and pass=2 branches share a ~25-line common prefix and differ only
 * in the tail. Before collapsing that duplication we pin the EXACT arg arrays
 * produced today, for every meaningful branch, so the refactor is provably
 * behavior-preserving.
 */
describe('VideoService.createTwoPassesVideoEncodingArgs (characterization)', () => {
  let service: EncodingArgsService;

  const parsedInput = path.parse('/transcode/42/source.mkv');

  const baseSourceInfo = {
    fps: 24,
    codec: 'h264',
    bitrate: 5000,
    width: 1920,
    height: 1080
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

  const buildOptions = (overrides: Record<string, unknown> = {}) => ({
    inputFile: '/transcode/42/source.mkv',
    parsedInput,
    codec: VideoCodec.VP9,
    quality: 1080,
    videoParams: ['-c:v', 'libvpx-vp9'],
    sourceInfo: baseSourceInfo,
    crfKey: 'cq',
    advancedSettings,
    encodingSetting,
    outputFileName: 'output_1080.webm',
    ...overrides
  });

  const callPass = (pass: 1 | 2, overrides: Record<string, unknown> = {}): string[] =>
    (service as any).createTwoPassesVideoEncodingArgs({ ...buildOptions(overrides), pass });

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        VideoService,
        EncodingArgsService,
        {
          provide: WINSTON_MODULE_PROVIDER,
          useValue: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn(), notice: jest.fn() }
        },
        { provide: getQueueToken(TaskQueue.VIDEO_TRANSCODE_RESULT), useValue: { add: jest.fn(), remove: jest.fn() } },
        { provide: ConfigService, useValue: { get: jest.fn().mockReturnValue(undefined) } },
        { provide: DaplexApiService, useValue: {} },
        { provide: TranscoderApiService, useValue: {} }
      ]
    }).compile();

    service = module.get<EncodingArgsService>(EncodingArgsService);
  });

  describe('VP9 (production path)', () => {
    it('pinned pass 1 args', () => {
      expect(callPass(1)).toMatchSnapshot();
    });

    it('pinned pass 2 args', () => {
      expect(callPass(2)).toMatchSnapshot();
    });

    it('pass 1 ends with the null-output two-pass tail', () => {
      const args = callPass(1);
      const nullSink = process.platform === 'win32' ? 'NUL' : '/dev/null';
      expect(args.slice(-6)).toEqual(['-pass', '1', '-an', '-f', 'null', nullSink]);
      expect(args).not.toContain('-map_chapters');
    });

    it('pass 2 ends with the output-file two-pass tail and strips chapters', () => {
      const args = callPass(2);
      expect(args.slice(-3)).toEqual(['-pass', '2', `"${parsedInput.dir}/output_1080.webm"`]);
      expect(args).toContain('-map_chapters');
    });

    it('pass 1 and pass 2 share an identical common prefix up to the -map flag', () => {
      const cut = (args: string[]) => args.slice(0, args.indexOf('-map'));
      expect(cut(callPass(1))).toEqual(cut(callPass(2)));
    });

    it('both passes use the same non-segment passlogfile', () => {
      const logfile = `"${parsedInput.dir}/${parsedInput.name}_2pass.log"`;
      expect(callPass(1)).toContain(logfile);
      expect(callPass(2)).toContain(logfile);
    });
  });

  describe('H264 branch', () => {
    it('pinned pass 1 args', () => {
      expect(callPass(1, { codec: VideoCodec.H264, crfKey: 'crf' })).toMatchSnapshot();
    });
    it('pinned pass 2 args', () => {
      expect(callPass(2, { codec: VideoCodec.H264, crfKey: 'crf' })).toMatchSnapshot();
    });
  });

  describe('AV1 branch', () => {
    it('pinned pass 1 args', () => {
      expect(callPass(1, { codec: VideoCodec.AV1, crfKey: 'cq' })).toMatchSnapshot();
    });
    it('pinned pass 2 args', () => {
      expect(callPass(2, { codec: VideoCodec.AV1, crfKey: 'cq' })).toMatchSnapshot();
    });
  });

  describe('URL input branch (UseURLInput = true)', () => {
    beforeEach(() => {
      (service as any).UseURLInput = true;
    });
    it('pinned pass 1 args (reconnect flags present)', () => {
      expect(callPass(1)).toMatchSnapshot();
    });
    it('pinned pass 2 args (reconnect flags present)', () => {
      expect(callPass(2)).toMatchSnapshot();
    });
  });

  describe('segment index branch', () => {
    it('pinned pass 1 args (split-segment passlogfile)', () => {
      expect(callPass(1, { segmentIndex: 0 })).toMatchSnapshot();
    });
    it('pinned pass 2 args (split-segment passlogfile)', () => {
      expect(callPass(2, { segmentIndex: 0 })).toMatchSnapshot();
    });
  });

  describe('split-from / split-duration branch', () => {
    it('pinned pass 1 args (seek + duration)', () => {
      expect(callPass(1, { splitFrom: '10.5', splitDuration: '30' })).toMatchSnapshot();
    });
    it('pinned pass 2 args (seek + duration)', () => {
      expect(callPass(2, { splitFrom: '10.5', splitDuration: '30' })).toMatchSnapshot();
    });
  });
});
