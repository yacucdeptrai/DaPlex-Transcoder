import { Test, TestingModule } from '@nestjs/testing';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';

import { DaplexApiService } from './daplex-api.service';

describe('DaplexApiService', () => {
  let service: DaplexApiService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        DaplexApiService,
        { provide: WINSTON_MODULE_PROVIDER, useValue: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn() } },
        { provide: HttpService, useValue: {} },
        { provide: ConfigService, useValue: { get: jest.fn() } }
      ]
    }).compile();

    service = module.get<DaplexApiService>(DaplexApiService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
