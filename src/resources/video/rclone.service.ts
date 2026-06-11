import { Inject, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectModel } from '@nestjs/mongoose';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';
import { Job } from 'bullmq';
import { Model } from 'mongoose';
import path from 'path';

import { IExternalStorage } from '../../models/external-storage.model';
import { fileHelper, rcloneHelper, StringCrypto } from '../../utils';
import { IStorage, IVideoData } from './interfaces';

/**
 * Called when the storage record is missing. Returns the status object whose
 * errorCode is thrown — lets VideoService keep ownership of the result-queue
 * emission while the rclone/credential work lives here.
 */
type StorageNotFoundHandler = (job: Job<IVideoData>) => Promise<{ errorCode: string }>;

/**
 * Instance-method layer over the stateless rcloneHelper. Owns the credential
 * paths: decrypting the storage secret and assembling the on-disk rclone
 * config. Secrets reach rclone only via the config file, never argv or logs.
 */
@Injectable()
export class RcloneService {
  constructor(
    @Inject(WINSTON_MODULE_PROVIDER) private readonly logger: Logger,
    private configService: ConfigService,
    @InjectModel('externalstorage') private externalStorageModel: Model<IExternalStorage>
  ) {}

  async ensureRcloneConfigExist(
    configFile: string,
    storage: string,
    job: Job<IVideoData>,
    onStorageNotFound: StorageNotFoundHandler
  ) {
    const configExists = await fileHelper.findInFile(configFile, `[${storage}]`);
    if (!configExists) {
      this.logger.info(`Config for remote "${storage}" not found, generating...`);
      let externalStorage = await this.externalStorageModel
        .findOne({ _id: BigInt(storage) })
        .lean()
        .exec();
      if (!externalStorage) {
        const statusError = await onStorageNotFound(job);
        throw new Error(statusError.errorCode);
      }
      externalStorage = await this.decryptToken(externalStorage);
      const newConfig = rcloneHelper.createRcloneConfig(externalStorage);
      await fileHelper.appendToFile(configFile, newConfig);
      this.logger.info(`Generated config for remote "${storage}"`);
    }
  }

  async getLinkedSourceUrl(job: Job<IVideoData>, onStorageNotFound: StorageNotFoundHandler) {
    let externalStorage;
    if (job.data.linkedStorage)
      externalStorage = await this.externalStorageModel
        .findOne({ _id: BigInt(job.data.linkedStorage) }, { publicUrl: 1, folderId: 1 })
        .lean()
        .exec();
    else
      externalStorage = await this.externalStorageModel
        .findOne({ _id: BigInt(job.data.storage) }, { publicUrl: 1, folderId: 1 })
        .lean()
        .exec();
    if (!externalStorage) {
      const statusError = await onStorageNotFound(job);
      throw new Error(statusError.errorCode);
    }
    if (!externalStorage.publicUrl) return null;
    const sourcePathItems = [externalStorage.folderId || '', job.data.path, job.data.filename];
    const sourcePath = path.posix.join(...sourcePathItems.map((value) => encodeURIComponent(value)));
    return externalStorage.publicUrl.replace(':service_path', 's3').replace(':path', sourcePath);
  }

  private async decryptToken(storage: IStorage) {
    const stringCrypto = new StringCrypto(this.configService.get<string>('CRYPTO_SECRET_KEY'));
    storage.clientSecret = await stringCrypto.decrypt(storage.clientSecret);
    return storage;
  }
}
