import { Test, TestingModule } from '@nestjs/testing';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';

import { VideoConsumerH264 } from './video.consumer';
import { VideoService } from './video.service';

describe('VideoConsumer', () => {
  let controller: VideoConsumerH264;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        VideoConsumerH264,
        { provide: WINSTON_MODULE_PROVIDER, useValue: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn() } },
        { provide: VideoService, useValue: {} }
      ]
    }).compile();

    controller = module.get<VideoConsumerH264>(VideoConsumerH264);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
