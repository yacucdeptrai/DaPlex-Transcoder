import { Inject } from '@nestjs/common';
import { OnWorkerEvent, Processor, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';

import { VideoService } from './video.service';
import { TaskQueue } from '../../enums/task-queue.enum';
import { IVideoData } from './interfaces/video-data.interface';
import { VideoCodec } from '../../enums/video-codec.enum';

export abstract class BaseVideoConsumer extends WorkerHost {
  constructor(@Inject(WINSTON_MODULE_PROVIDER) protected readonly logger: Logger) {
    super();
  }

  async pauseWorker(): Promise<void> {
    if (this.worker.isPaused()) return;
    await this.worker.pause();
  }

  resumeWorker(): void {
    if (!this.worker.isPaused()) return;
    this.worker.resume();
  }

  async closeWorker(): Promise<void> {
    if (!this.worker.isRunning()) return;
    await this.worker.close();
  }

  @OnWorkerEvent('paused')
  onPaused() {
    this.logger.info('Worker has been paused');
  }

  @OnWorkerEvent('resumed')
  onResumed() {
    this.logger.info('Worker has been resumed');
  }

  @OnWorkerEvent('closed')
  onClosed() {
    this.logger.info('Worker has been closed');
  }
}

@Processor(`${TaskQueue.VIDEO_TRANSCODE}:${VideoCodec.H264}`, { concurrency: 1 })
export class VideoConsumerH264 extends BaseVideoConsumer {
  constructor(
    @Inject(WINSTON_MODULE_PROVIDER) protected readonly logger: Logger,
    private readonly videoService: VideoService
  ) {
    super(logger);
  }

  async process(job: Job<IVideoData, any, string>) {
    const result = await this.videoService.transcode(job, VideoCodec.H264);
    return result;
  }

  @OnWorkerEvent('active')
  onActive(job: Job) {
    this.logger.info(`Processing job ${job.id} of type H264`);
  }
}

@Processor(`${TaskQueue.VIDEO_TRANSCODE}:${VideoCodec.H265}`, { concurrency: 1 })
export class VideoConsumerH265 extends BaseVideoConsumer {
  constructor(
    @Inject(WINSTON_MODULE_PROVIDER) protected readonly logger: Logger,
    private readonly videoService: VideoService
  ) {
    super(logger);
  }

  async process(job: Job<IVideoData, any, string>) {
    const result = await this.videoService.transcode(job, VideoCodec.H265);
    return result;
  }

  @OnWorkerEvent('active')
  onActive(job: Job) {
    this.logger.info(`Processing job ${job.id} of type H265`);
  }
}

@Processor(`${TaskQueue.VIDEO_TRANSCODE}:${VideoCodec.VP9}`, { concurrency: 1 })
export class VideoConsumerVP9 extends BaseVideoConsumer {
  constructor(
    @Inject(WINSTON_MODULE_PROVIDER) protected readonly logger: Logger,
    private readonly videoService: VideoService
  ) {
    super(logger);
  }

  async process(job: Job<IVideoData, any, string>) {
    const result = await this.videoService.transcode(job, VideoCodec.VP9);
    return result;
  }

  @OnWorkerEvent('active')
  onActive(job: Job) {
    this.logger.info(`Processing job ${job.id} of type VP9`);
  }
}

@Processor(`${TaskQueue.VIDEO_TRANSCODE}:${VideoCodec.AV1}`, { concurrency: 1 })
export class VideoConsumerAV1 extends BaseVideoConsumer {
  constructor(
    @Inject(WINSTON_MODULE_PROVIDER) protected readonly logger: Logger,
    private readonly videoService: VideoService
  ) {
    super(logger);
  }

  async process(job: Job<IVideoData, any, string>) {
    const result = await this.videoService.transcode(job, VideoCodec.AV1);
    return result;
  }

  @OnWorkerEvent('active')
  onActive(job: Job) {
    this.logger.info(`Processing job ${job.id} of type AV1`);
  }
}
