import { Test, TestingModule } from '@nestjs/testing';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';

import { TranscoderApiService } from './transcoder-api.service';

describe('TranscoderApiService', () => {
  let service: TranscoderApiService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        TranscoderApiService,
        { provide: WINSTON_MODULE_PROVIDER, useValue: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn() } },
        { provide: HttpService, useValue: {} },
        { provide: ConfigService, useValue: { get: jest.fn() } }
      ]
    }).compile();

    service = module.get<TranscoderApiService>(TranscoderApiService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
