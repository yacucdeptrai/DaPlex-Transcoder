import { Test, TestingModule } from '@nestjs/testing';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';

import {
  BaseVideoConsumer,
  VideoConsumerAV1,
  VideoConsumerH264,
  VideoConsumerH265,
  VideoConsumerVP9
} from './video.consumer';
import { VideoService } from './video.service';
import { TaskQueue } from '../../enums/task-queue.enum';
import { VideoCodec } from '../../enums/video-codec.enum';

// @nestjs/bullmq metadata keys (literal — avoids coupling to the package's internal dist path).
const PROCESSOR_METADATA = 'bullmq:processor_metadata';
const ON_WORKER_EVENT_METADATA = 'bullmq:worker_events_metadata';

type ConsumerCtor = new (...args: any[]) => BaseVideoConsumer;

interface CodecCase {
  readonly name: string;
  readonly Ctor: ConsumerCtor;
  readonly codec: VideoCodec;
  readonly label: string;
}

// One row per concrete consumer. The label is the exact suffix the original
// per-class onActive() logged ("Processing job <id> of type <label>") and the
// value VideoCodec[codec] must reverse-map to after consolidation.
const CASES: readonly CodecCase[] = [
  { name: 'VideoConsumerH264', Ctor: VideoConsumerH264, codec: VideoCodec.H264, label: 'H264' },
  { name: 'VideoConsumerH265', Ctor: VideoConsumerH265, codec: VideoCodec.H265, label: 'H265' },
  { name: 'VideoConsumerVP9', Ctor: VideoConsumerVP9, codec: VideoCodec.VP9, label: 'VP9' },
  { name: 'VideoConsumerAV1', Ctor: VideoConsumerAV1, codec: VideoCodec.AV1, label: 'AV1' }
];

describe('VideoConsumer', () => {
  describe.each(CASES)('$name', ({ Ctor, codec, label }) => {
    let consumer: BaseVideoConsumer;
    let logger: { info: jest.Mock; error: jest.Mock; warn: jest.Mock; debug: jest.Mock };
    let videoService: { transcode: jest.Mock };

    beforeEach(async () => {
      logger = { info: jest.fn(), error: jest.fn(), warn: jest.fn(), debug: jest.fn() };
      videoService = { transcode: jest.fn() };
      const module: TestingModule = await Test.createTestingModule({
        providers: [
          Ctor,
          { provide: WINSTON_MODULE_PROVIDER, useValue: logger },
          { provide: VideoService, useValue: videoService }
        ]
      }).compile();

      consumer = module.get(Ctor);
    });

    it('should be defined', () => {
      expect(consumer).toBeDefined();
    });

    it('process() delegates to videoService.transcode with this codec and returns its result', async () => {
      const job: any = { id: '42', data: { foo: 'bar' } };
      const expected = Symbol('transcode-result');
      videoService.transcode.mockResolvedValue(expected);

      const result = await consumer.process(job);

      expect(videoService.transcode).toHaveBeenCalledTimes(1);
      expect(videoService.transcode).toHaveBeenCalledWith(job, codec);
      expect(result).toBe(expected);
    });

    it('onActive() logs the processing message with this codec label', () => {
      const job: any = { id: '99' };

      (consumer as any).onActive(job);

      expect(logger.info).toHaveBeenCalledTimes(1);
      expect(logger.info).toHaveBeenCalledWith(`Processing job ${job.id} of type ${label}`);
    });
  });

  describe('decorator metadata (queue binding + event registration)', () => {
    it('each consumer keeps its own distinct @Processor queue name', () => {
      const names = CASES.map(({ Ctor }) => Reflect.getMetadata(PROCESSOR_METADATA, Ctor)?.name);

      expect(names).toEqual([
        `${TaskQueue.VIDEO_TRANSCODE}:${VideoCodec.H264}`,
        `${TaskQueue.VIDEO_TRANSCODE}:${VideoCodec.H265}`,
        `${TaskQueue.VIDEO_TRANSCODE}:${VideoCodec.VP9}`,
        `${TaskQueue.VIDEO_TRANSCODE}:${VideoCodec.AV1}`
      ]);
      // distinct => 4 workers bind to 4 different queues
      expect(new Set(names).size).toBe(CASES.length);
    });

    it.each(CASES)('$name onActive carries @OnWorkerEvent(active) metadata (own or inherited)', ({ Ctor }) => {
      // prototype property access resolves through the chain, so this passes whether
      // onActive lives on the subclass (before) or on BaseVideoConsumer (after).
      const meta = Reflect.getMetadata(ON_WORKER_EVENT_METADATA, Ctor.prototype.onActive);
      expect(meta).toEqual({ eventName: 'active' });
    });

    it.each(CASES)('$name still discovers the inherited worker-lifecycle handlers', ({ Ctor }) => {
      expect(Reflect.getMetadata(ON_WORKER_EVENT_METADATA, Ctor.prototype.onPaused)).toEqual({ eventName: 'paused' });
      expect(Reflect.getMetadata(ON_WORKER_EVENT_METADATA, Ctor.prototype.onResumed)).toEqual({ eventName: 'resumed' });
      expect(Reflect.getMetadata(ON_WORKER_EVENT_METADATA, Ctor.prototype.onClosed)).toEqual({ eventName: 'closed' });
    });
  });
});
