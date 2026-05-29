import { Test, TestingModule } from '@nestjs/testing';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';

import { RedisPubSubService } from './redis-pubsub.service';
import { REDIS_PUBSUB_CONFIG } from './redis-pubsub.constants';

describe('RedisPubsubService', () => {
  let service: RedisPubSubService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        RedisPubSubService,
        { provide: REDIS_PUBSUB_CONFIG, useValue: {} },
        { provide: DiscoveryService, useValue: {} },
        { provide: MetadataScanner, useValue: {} }
      ]
    }).compile();

    service = module.get<RedisPubSubService>(RedisPubSubService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
