import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';

import { DaplexApiService } from './daplex-api.service';

@Module({
  imports: [HttpModule],
  providers: [DaplexApiService],
  exports: [DaplexApiService]
})
export class DaplexApiModule { }
