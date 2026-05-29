import { Test, TestingModule } from '@nestjs/testing';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';

import { VideoCancelConsumer } from './video-cancel.consumer';
import { VideoService } from '../video/video.service';

describe('VideoCancelConsumer', () => {
  let controller: VideoCancelConsumer;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        VideoCancelConsumer,
        { provide: WINSTON_MODULE_PROVIDER, useValue: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn() } },
        { provide: VideoService, useValue: {} }
      ]
    }).compile();

    controller = module.get<VideoCancelConsumer>(VideoCancelConsumer);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
