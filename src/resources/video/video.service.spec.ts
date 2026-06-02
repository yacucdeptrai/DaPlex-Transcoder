import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { getQueueToken } from '@nestjs/bullmq';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';

import { VideoService } from './video.service';
import { TaskQueue } from '../../enums';
import { DaplexApiService } from '../../common/modules/daplex-api';
import { TranscoderApiService } from '../../common/modules/transcoder-api';
import { EncodingArgsService } from './encoding-args.service';

describe('VideoService', () => {
  let service: VideoService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        VideoService,
        EncodingArgsService,
        {
          provide: WINSTON_MODULE_PROVIDER,
          useValue: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn() }
        },
        { provide: getQueueToken(TaskQueue.VIDEO_TRANSCODE_RESULT), useValue: { add: jest.fn(), remove: jest.fn() } },
        { provide: ConfigService, useValue: { get: jest.fn() } },
        { provide: DaplexApiService, useValue: {} },
        { provide: TranscoderApiService, useValue: {} }
      ]
    }).compile();

    service = module.get<VideoService>(VideoService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
