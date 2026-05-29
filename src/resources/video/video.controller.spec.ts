import { Test, TestingModule } from '@nestjs/testing';

import { VideoController } from './video.controller';
import { BaseVideoConsumer } from './video.consumer';
import { VideoService } from './video.service';

describe('VideoController', () => {
  let controller: VideoController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [VideoController],
      providers: [
        { provide: BaseVideoConsumer, useValue: {} },
        { provide: VideoService, useValue: {} }
      ]
    }).compile();

    controller = module.get<VideoController>(VideoController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
