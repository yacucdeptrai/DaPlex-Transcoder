import { HttpService } from '@nestjs/axios';
import { Inject, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { firstValueFrom } from 'rxjs';
import { Logger } from 'winston';

import { fileHelper } from '../../../utils';
import { BYPASS_PRODUCER_CHECK_FILE, PRODUCER_DOMAINS_FILE } from '../../../config';

@Injectable()
export class DaplexApiService {
  constructor(@Inject(WINSTON_MODULE_PROVIDER) private readonly logger: Logger, private httpService: HttpService,
    private configService: ConfigService) { }

  async ensureProducerAppIsOnline(url: string, retries: number = 10, retryTimeout: number = 30000) {
    this.logger.info(`Pinging producer: ${url}`);
    let totalRetries = 0;
    while (totalRetries < retries) {
      if (totalRetries > 0)
        this.logger.info(`Retrying ${totalRetries / retries}`);
      const bypassCheckFileExist = await fileHelper.fileExists(BYPASS_PRODUCER_CHECK_FILE);
      if (bypassCheckFileExist) {
        this.logger.info('Bypass producer check file detected, skipping...');
        return true;
      }
      const isValidUrl = await this.isValidApiUrl(url);
      if (!isValidUrl) {
        this.logger.warning(`Invalid producer url detected, retrying in ${retryTimeout}ms`);
        await new Promise(r => setTimeout(r, retryTimeout));
        totalRetries++;
        continue;
      }
      try {
        const response = await firstValueFrom(this.httpService.get(url, {
          headers: { 'Content-Type': 'application/json' }, family: 4
        }));
        this.logger.info(`GET ${url}: ${response.status}`);
        return true;
      } catch (e) {
        totalRetries++;
        if (e.isAxiosError) {
          this.logger.error(`Failed to validate online status of the producer app, retrying in ${retryTimeout}ms`);
          await new Promise(r => setTimeout(r, retryTimeout));
        } else {
          this.logger.error(e);
          throw e;
        }
      }
    }
  }

  private async isValidApiUrl(url: string) {
    const envApiDomains = (this.configService.get<string>('DAPLEX_API_DOMAINS') || '').split(',');
    let urlData: URL;
    try {
      urlData = new URL(url);
    } catch {
      return false;
    }
    if (envApiDomains.includes(urlData.hostname)) {
      return true;
    }
    const fileApiDomains = await fileHelper.readAllLines(PRODUCER_DOMAINS_FILE);
    if (fileApiDomains.includes(urlData.hostname)) {
      return true;
    }
    return false;
  }
}
