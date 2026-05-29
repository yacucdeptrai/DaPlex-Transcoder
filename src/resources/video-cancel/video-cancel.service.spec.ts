import { Test, TestingModule } from '@nestjs/testing';

import { VideoCancelService } from './video-cancel.service';
import { VideoService } from '../video/video.service';

describe('VideoCancelService', () => {
  let service: VideoCancelService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [VideoCancelService, { provide: VideoService, useValue: {} }]
    }).compile();

    service = module.get<VideoCancelService>(VideoCancelService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
