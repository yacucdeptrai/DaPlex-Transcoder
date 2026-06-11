import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { getQueueToken } from '@nestjs/bullmq';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import * as path from 'path';

import { VideoService } from './video.service';
import { EncodingArgsService } from './encoding-args.service';
import { QualityResolverService } from './quality-resolver.service';
import { ProcessSpawnerService } from './process-spawner.service';
import { CodecPresetRegistry } from './codec-preset.registry';
import { TaskQueue, VideoCodec } from '../../enums';
import { DaplexApiService } from '../../common/modules/daplex-api';
import { TranscoderApiService } from '../../common/modules/transcoder-api';

/**
 * Characterization tests for the SVT-AV1 preset dispatch slated to become
 * CodecPresetRegistry (Slice B). resolveSVTAV1Params selects one of three param
 * tables by the SVT_AV1_PRESET config value (psy / hdr / default-main) and then
 * appends scm=0, the hdr/luminance tail and keyint. The existing encoding-args
 * snapshots only exercise the default `main` table (config undefined); this pins
 * the `psy` and `hdr` branches too, so extracting the table cannot silently
 * change which preset a config value selects.
 *
 * Uses concrete-value assertions on the -svtav1-params arg (no snapshot — the
 * value is platform-independent and the per-preset table is the load-bearing part).
 */
describe('EncodingArgsService SVT-AV1 preset dispatch (characterization)', () => {
  const parsedInput = path.parse('/transcode/42/source.mkv');

  const baseSourceInfo = {
    fps: 24,
    codec: 'h264',
    bitrate: 5000,
    width: 1920,
    height: 1080,
    isHDR: false,
    hdrParams: null
  } as any;

  const buildTarget = async (svtPreset: string | undefined) => {
    const configValues: Record<string, string | undefined> = {
      RCLONE_CONFIG_FILE: '/config/rclone.conf',
      SVT_AV1_PRESET: svtPreset
    };
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        VideoService,
        EncodingArgsService,
        QualityResolverService,
        ProcessSpawnerService,
        CodecPresetRegistry,
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
    return module.get<EncodingArgsService>(EncodingArgsService) as any;
  };

  const svtParamsOf = (args: string[]): string => {
    const i = args.indexOf('-svtav1-params');
    expect(i).toBeGreaterThan(-1);
    // The value is wrapped in double quotes by the builder.
    return args[i + 1].replace(/^"|"$/g, '');
  };

  const callAv1 = (target: any): string[] =>
    target.createVideoEncodingArgs({
      inputFile: '/transcode/42/source.mkv',
      parsedInput,
      codec: VideoCodec.AV1,
      quality: 1080,
      videoParams: ['-c:v', 'libsvtav1'],
      sourceInfo: baseSourceInfo,
      crfKey: 'cq',
      advancedSettings: { h264Tune: '' },
      encodingSetting: { crf: 23, h265Crf: 28, cq: 30, useLowerRate: false, maxrate: 8000, bufsize: 16000 },
      outputFileName: 'v_1080.webm'
    });

  it('default (config undefined) selects the main preset table', async () => {
    const target = await buildTarget(undefined);
    expect(svtParamsOf(callAv1(target))).toBe(
      'tune=0:enable-overlays=1:film-grain=0:film-grain-denoise=0:scd=1:sharpness=0:enable-qm=1:qm-min=0:enable-variance-boost=1:scm=0:luminance-qp-bias=30:keyint=48'
    );
  });

  it('SVT_AV1_PRESET=psy selects the psy preset table', async () => {
    const target = await buildTarget('psy');
    expect(svtParamsOf(callAv1(target))).toBe(
      'tune=0:enable-overlays=1:film-grain=0:film-grain-denoise=0:sharpness=0:scd=1:scm=0:luminance-qp-bias=30:keyint=48'
    );
  });

  it('SVT_AV1_PRESET=hdr selects the hdr preset table', async () => {
    const target = await buildTarget('hdr');
    expect(svtParamsOf(callAv1(target))).toBe('sharpness=0:scm=0:luminance-qp-bias=30:keyint=48');
  });

  it('an unknown preset value falls back to the main table', async () => {
    const target = await buildTarget('nonsense');
    expect(svtParamsOf(callAv1(target))).toBe(
      'tune=0:enable-overlays=1:film-grain=0:film-grain-denoise=0:scd=1:sharpness=0:enable-qm=1:qm-min=0:enable-variance-boost=1:scm=0:luminance-qp-bias=30:keyint=48'
    );
  });
});
