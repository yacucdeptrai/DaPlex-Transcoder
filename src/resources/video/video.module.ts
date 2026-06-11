import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { ConfigService } from '@nestjs/config';
import { MongooseModule } from '@nestjs/mongoose';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';

import { settingSchema } from '../../models/setting.model';
import { mediaSchema } from '../../models/media.model';
import { externalStorageSchema } from '../../models/external-storage.model';
import { mediaStorageSchema } from '../../models/media-storage.model';
import { VideoService } from './video.service';
import { EncodingArgsService } from './encoding-args.service';
import { QualityResolverService } from './quality-resolver.service';
import { ProcessSpawnerService } from './process-spawner.service';
import { CodecPresetRegistry } from './codec-preset.registry';
import { RcloneService } from './rclone.service';
import {
  BaseVideoConsumer,
  VideoConsumerAV1,
  VideoConsumerH264,
  VideoConsumerH265,
  VideoConsumerVP9
} from './video.consumer';
import { DaplexApiModule } from '../../common/modules/daplex-api';
import { TranscoderApiModule } from '../../common/modules/transcoder-api';
import { TaskQueue, VideoCodec } from '../../enums';
import { VideoController } from './video.controller';

function getTargetConsumer(consumerCodec: number) {
  if (consumerCodec === VideoCodec.H265) return VideoConsumerH265;
  if (consumerCodec === VideoCodec.AV1) return VideoConsumerAV1;
  else if (consumerCodec === VideoCodec.VP9) return VideoConsumerVP9;
  return VideoConsumerH264;
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
    MongooseModule.forFeature([
      { name: 'setting', schema: settingSchema },
      { name: 'media', schema: mediaSchema },
      { name: 'externalstorage', schema: externalStorageSchema },
      { name: 'mediastorage', schema: mediaStorageSchema }
    ]),
    DaplexApiModule,
    TranscoderApiModule
  ],
  providers: [
    VideoService,
    EncodingArgsService,
    QualityResolverService,
    ProcessSpawnerService,
    CodecPresetRegistry,
    RcloneService,
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
export class VideoModule {}
