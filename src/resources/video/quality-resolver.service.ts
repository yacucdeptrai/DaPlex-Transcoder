import { Inject, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';
import { Job } from 'bullmq';
import mongoose from 'mongoose';
import path from 'path';

import { mediaStorageModel } from '../../models/media-storage.model';
import { rcloneHelper } from '../../utils';
import { HlsManifest } from '../../common/interfaces';
import { IVideoData } from './interfaces';

/**
 * Resolves which video qualities still need to be encoded for a source, by
 * combining the source height (calculateQuality), already-encoded DB stream
 * records (findAvailableQuality), and any existing remote HLS manifest
 * (findExistingManifest). The orchestration that ties them together
 * (validateSourceQuality) stays in VideoService because it also touches the
 * result queue + status.
 */
@Injectable()
export class QualityResolverService {
  constructor(@Inject(WINSTON_MODULE_PROVIDER) private readonly logger: Logger, private configService: ConfigService) {}

  async findAvailableQuality(
    uploadedFiles: string[],
    allQualityList: number[],
    parsedInput: path.ParsedPath,
    codec: number,
    replaceStreams: string[] = [],
    job: Job<IVideoData>
  ) {
    const fileIds: bigint[] = [];
    for (let i = 0; i < uploadedFiles.length; i++) {
      const uploadedFileName = uploadedFiles[i].split('/').pop();
      if (!allQualityList.find((q) => uploadedFileName === `${parsedInput.name}_${q}.mp4`)) continue;
      const stringId = uploadedFiles[i].split('/')[0];
      if (replaceStreams.includes(stringId)) continue;
      if (isNaN(<any>stringId)) continue;
      fileIds.push(BigInt(stringId));
    }
    await mongoose.connect(this.configService.get<string>('DATABASE_URL'), { family: 4, useBigInt64: true });
    const sourceFileMeta = await mediaStorageModel
      .findOne({ _id: BigInt(job.data._id) })
      .lean()
      .exec();
    await mongoose.disconnect();
    const qualityList = sourceFileMeta.streams
      .filter((file) => file.codec === codec && fileIds.includes(file._id))
      .map((file) => file.quality);
    const availableQualityList = allQualityList.filter((quality) => !qualityList.includes(quality));
    return availableQualityList;
  }

  calculateQuality(
    height: number,
    qualityList: number[],
    forcedQualityList: number[] = [],
    fallbackQualityList: number[] = []
  ) {
    const availableQualityList: number[] = [];
    if (!height) return availableQualityList;
    for (let i = 0; i < qualityList.length; i++) {
      if (height >= qualityList[i] || forcedQualityList.includes(qualityList[i])) {
        availableQualityList.push(qualityList[i]);
      }
    }
    // Use the lowest quality when there is no suitable one
    if (!availableQualityList.length) availableQualityList.push(...fallbackQualityList);
    return availableQualityList;
  }

  async findExistingManifest(remote: string, parentFolder: string, codec: number) {
    const rcloneConfigFile = this.configService.get<string>('RCLONE_CONFIG_FILE');
    const rcloneDir = this.configService.get<string>('RCLONE_DIR');
    const isFolderExist = await rcloneHelper.isPathExist(rcloneConfigFile, rcloneDir, remote, parentFolder);
    if (!isFolderExist) return null;
    const [manifestFileInfo] = await rcloneHelper.listRemoteJson(rcloneConfigFile, rcloneDir, remote, parentFolder, {
      filesOnly: true,
      recursive: true,
      include: `*/manifest_${codec}.json`
    });
    if (!manifestFileInfo) return null;
    this.logger.info(`Found existing manifest from ${manifestFileInfo.Path}, reading data...`);
    const manifestContent = await rcloneHelper.readRemoteFile(
      rcloneConfigFile,
      rcloneDir,
      remote,
      parentFolder,
      manifestFileInfo.Path,
      (args) => {
        this.logger.info('rclone ' + args.join(' '));
      }
    );
    if (!manifestContent) return null;
    return <HlsManifest>JSON.parse(manifestContent);
  }
}
