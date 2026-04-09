import { Test, TestingModule } from '@nestjs/testing';
import { DaplexApiService } from './daplex-api.service';

describe('DaplexApiService', () => {
  let service: DaplexApiService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [DaplexApiService],
    }).compile();

    service = module.get<DaplexApiService>(DaplexApiService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
