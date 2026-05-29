import { Test, TestingModule } from '@nestjs/testing';
import { VideoConsumerH264 } from './video.consumer';
import { VideoService } from './video.service';

describe('VideoConsumer', () => {
  let controller: VideoConsumerH264;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [VideoConsumerH264, VideoService],
    }).compile();

    controller = module.get<VideoConsumerH264>(VideoConsumerH264);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
