import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { ConfigService } from '@nestjs/config';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';

import { VideoService } from './video.service';
import { BaseVideoConsumer, VideoCosumerAV1, VideoCosumerH264, VideoCosumerH265, VideoCosumerVP9 } from './video.consumer';
import { DaplexApiModule } from '../../common/modules/daplex-api';
import { TranscoderApiModule } from '../../common/modules/transcoder-api';
import { TaskQueue, VideoCodec } from '../../enums';
import { VideoController } from './video.controller';

function getTargetConsumer(consumerCodec: number) {
  if (consumerCodec === VideoCodec.H265)
    return VideoCosumerH265;
  if (consumerCodec === VideoCodec.AV1)
    return VideoCosumerAV1;
  else if (consumerCodec === VideoCodec.VP9)
    return VideoCosumerVP9;
  return VideoCosumerH264;
}

@Module({
  imports: [
    BullModule.registerQueue({
      name: TaskQueue.VIDEO_TRANSCODE_RESULT,
      defaultJobOptions: {
        removeOnComplete: true,
        removeOnFail: true,
        attempts: 3
      }
    }),
    DaplexApiModule,
    TranscoderApiModule
  ],
  providers: [
    VideoService,
    {
      provide: BaseVideoConsumer,
      useFactory: (configService: ConfigService, logger: Logger, videoService: VideoService) => {
        const consumerCodec = +configService.get<string>('VIDEO_CODEC');
        const ctr = getTargetConsumer(consumerCodec);
        return new ctr(logger, videoService);
      },
      inject: [ConfigService, WINSTON_MODULE_PROVIDER, VideoService]
    }
  ],
  exports: [VideoService],
  controllers: [VideoController]
})
export class VideoModule { }
