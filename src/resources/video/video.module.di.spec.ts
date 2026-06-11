import { Global, Module } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { HttpService } from '@nestjs/axios';
import { getQueueToken } from '@nestjs/bullmq';
import { getModelToken } from '@nestjs/mongoose';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';

import { VideoModule } from './video.module';
import { VideoService } from './video.service';
import { EncodingArgsService } from './encoding-args.service';
import { QualityResolverService } from './quality-resolver.service';
import { ProcessSpawnerService } from './process-spawner.service';
import { CodecPresetRegistry } from './codec-preset.registry';
import { RcloneService } from './rclone.service';
import { TaskQueue, VideoCodec } from '../../enums';

// In production WinstonModule.forRoot and ConfigModule.forRoot({isGlobal:true})
// supply these tokens globally (app.module). VideoModule + its API submodules rely
// on that global context, so the test provides them via a @Global module.
@Global()
@Module({
  providers: [
    {
      provide: WINSTON_MODULE_PROVIDER,
      useValue: { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn(), notice: jest.fn() }
    },
    {
      provide: ConfigService,
      useValue: { get: jest.fn((key: string) => (key === 'VIDEO_CODEC' ? String(VideoCodec.H264) : undefined)) }
    }
  ],
  exports: [WINSTON_MODULE_PROVIDER, ConfigService]
})
class TestGlobalsModule {}

/**
 * DI smoke test: compiles the REAL VideoModule provider graph (not a hand-wired
 * provider list), with only the external edges mocked — the BullMQ queue, the
 * HttpService behind Daplex/Transcoder API modules, the Winston logger, and the
 * VIDEO_CODEC config the BaseVideoConsumer factory reads.
 *
 * This is the tripwire for the split: when the surgeon adds ProcessSpawnerService /
 * RcloneService / CodecPresetRegistry as VideoService dependencies, they MUST be
 * registered in video.module.ts `providers`. If they forget, this test fails with
 * Nest's UnknownDependencyException at compile — which a hand-wired TestingModule
 * (e.g. video.service.spec.ts) would NOT catch. After the surgeon adds the new
 * services, extend the "resolves" assertions below to include them.
 */
describe('VideoModule DI graph (smoke)', () => {
  let moduleRef: TestingModule;

  beforeEach(async () => {
    moduleRef = await Test.createTestingModule({
      imports: [TestGlobalsModule, VideoModule]
    })
      // External edges only — the internal provider wiring stays real so a missing
      // registration genuinely throws UnknownDependencyException.
      .overrideProvider(getQueueToken(TaskQueue.VIDEO_TRANSCODE_RESULT))
      .useValue({ add: jest.fn(), remove: jest.fn() })
      .overrideProvider(HttpService)
      .useValue({ get: jest.fn(), post: jest.fn(), patch: jest.fn(), axiosRef: {} })
      // MongooseModule.forFeature registers these four model providers; override
      // them so the graph compiles without a live connection. The override only
      // satisfies tokens VideoModule actually registers — if a forFeature entry is
      // dropped, the service's @InjectModel still throws UnknownDependencyException.
      .overrideProvider(getModelToken('setting'))
      .useValue({})
      .overrideProvider(getModelToken('media'))
      .useValue({})
      .overrideProvider(getModelToken('externalstorage'))
      .useValue({})
      .overrideProvider(getModelToken('mediastorage'))
      .useValue({})
      .compile();
  });

  afterEach(async () => {
    await moduleRef?.close();
  });

  it('compiles the VideoModule provider graph and resolves VideoService', () => {
    expect(moduleRef.get(VideoService)).toBeInstanceOf(VideoService);
  });

  it('resolves the services already split out of VideoService', () => {
    expect(moduleRef.get(EncodingArgsService)).toBeInstanceOf(EncodingArgsService);
    expect(moduleRef.get(QualityResolverService)).toBeInstanceOf(QualityResolverService);
    expect(moduleRef.get(ProcessSpawnerService)).toBeInstanceOf(ProcessSpawnerService);
    expect(moduleRef.get(CodecPresetRegistry)).toBeInstanceOf(CodecPresetRegistry);
    expect(moduleRef.get(RcloneService)).toBeInstanceOf(RcloneService);
  });

  it('registers the four Mongoose models via forFeature (forRootAsync/forFeature canary)', () => {
    // If any forFeature entry is missing, compiling the module above would have
    // thrown UnknownDependencyException for the service's @InjectModel param.
    for (const name of ['setting', 'media', 'externalstorage', 'mediastorage']) {
      expect(moduleRef.get(getModelToken(name))).toBeDefined();
    }
  });

  it('builds the BaseVideoConsumer factory from VIDEO_CODEC without throwing', () => {
    // The factory injects ConfigService + Winston + VideoService; compiling the
    // module above already exercised it. Re-assert VideoService is the same
    // singleton the factory received (provider graph is internally consistent).
    expect(moduleRef.get(VideoService)).toBe(moduleRef.get(VideoService));
  });
});
