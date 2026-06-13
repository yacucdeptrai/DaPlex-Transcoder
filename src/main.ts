import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, NestFastifyApplication } from '@nestjs/platform-fastify';
import { WINSTON_MODULE_NEST_PROVIDER } from 'nest-winston';

import { AppModule } from './app.module';
import { PORT, ADDRESS } from './config';
import { mongooseHelper } from './utils';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(AppModule, new FastifyAdapter({
    logger: true,
    disableRequestLogging: true
  }));
  app.enableCors();
  app.useLogger(app.get(WINSTON_MODULE_NEST_PROVIDER));
  const port = process.env.PORT || PORT;
  const address = process.env.ADDRESS || ADDRESS;
  app.enableShutdownHooks();
  await app.listen(port, address);
}

mongooseHelper.applyBigIntPatches();
bootstrap();
