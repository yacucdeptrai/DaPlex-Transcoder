import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { getQueueToken } from '@nestjs/bullmq';
import { getModelToken } from '@nestjs/mongoose';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';

import { VideoService } from './video.service';
import { TaskQueue } from '../../enums';
import { DaplexApiService } from '../../common/modules/daplex-api';
import { TranscoderApiService } from '../../common/modules/transcoder-api';
import { EncodingArgsService } from './encoding-args.service';
import { QualityResolverService } from './quality-resolver.service';
import { ProcessSpawnerService } from './process-spawner.service';
import { CodecPresetRegistry } from './codec-preset.registry';
import { RcloneService } from './rclone.service';

describe('VideoService', () => {
  let service: VideoService;

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
          useValue: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn() }
        },
        { provide: getQueueToken(TaskQueue.VIDEO_TRANSCODE_RESULT), useValue: { add: jest.fn(), remove: jest.fn() } },
        { provide: ConfigService, useValue: { get: jest.fn() } },
        { provide: DaplexApiService, useValue: {} },
        { provide: TranscoderApiService, useValue: {} },
        { provide: getModelToken('setting'), useValue: { findOne: jest.fn() } },
        { provide: getModelToken('media'), useValue: { findOne: jest.fn() } },
        { provide: getModelToken('externalstorage'), useValue: { findOne: jest.fn() } },
        { provide: getModelToken('mediastorage'), useValue: { findOne: jest.fn() } }
      ]
    }).compile();

    service = module.get<VideoService>(VideoService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
