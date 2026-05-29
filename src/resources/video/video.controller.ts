import { Controller, Get, HttpCode, Post } from '@nestjs/common';
import { RouteConfig } from '@nestjs/platform-fastify';

import { BaseVideoConsumer } from './video.consumer';
import { VideoService } from './video.service';

@Controller('video')
export class VideoController {
  constructor(private consumer: BaseVideoConsumer, private videoService: VideoService) { }

  @Post('pause')
  pauseWorker(): Promise<void> {
    return this.consumer.pauseWorker();
  }

  @Post('resume')
  resumeWorker(): void {
    return this.consumer.resumeWorker();
  }

  @Post('close')
  closeWorker(): Promise<void> {
    return this.consumer.closeWorker();
  }

  @Post('retry-encoding')
  @HttpCode(204)
  retryEncoding() {
    this.videoService.setRetryEncoding();
  }

  @Get('transcoder-priority')
  @RouteConfig({ logLevel: 'warn' })
  getTranscoderPriority() {
    const priority = this.videoService.getTranscoderPriority();
    return { priority };
  }
}
