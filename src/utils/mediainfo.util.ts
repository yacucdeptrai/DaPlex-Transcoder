import child_process from 'child_process';

import { stringHelper } from './string-helper.util';

export class MediaInfoHelper {
  knwonEncodingSettings = ['cabac', 'ref', 'deblock', 'analyse', 'me', 'subme', 'psy', 'psy_rd', 'mixed_ref', 'me_range',
    'chroma_me', 'trellis', '8x8dct', 'deadzone', 'fast_pskip', 'nr', 'decimate', 'interlaced', 'constrained_intra',
    'bframes', 'b_pyramid', 'b_adapt', 'b_bias', 'direct', 'weightb', 'weightp', 'scenecut', 'intra_refresh', 'rc_lookahead', 'mbtree',
    'nal_hrd', 'filler', 'ip_ratio', 'aq'
  ];

  multiResEncodingSettings = ['cabac', 'ref', 'deblock', 'analyse', 'me', 'subme', 'psy', 'psy_rd', 'mixed_ref', 'me_range',
    'trellis', '8x8dct', 'deadzone', 'fast_pskip', 'nr', 'decimate', 'interlaced', 'constrained_intra', 'bframes',
    'b_pyramid', 'b_bias', 'weightb', 'weightp', 'scenecut', 'intra_refresh', 'rc_lookahead', 'mbtree',
    'nal_hrd', 'filler', 'ip_ratio', 'aq'
  ];

  getMediaInfo(input: string, mediainfoDir: string) {
    const args: string[] = [
      `"${input}"`,
      '--output=JSON'
    ];
    return new Promise<MediaInfoResult>((resolve, reject) => {
      const mediainfo = child_process.spawn(`"${mediainfoDir}/mediainfo"`, args, { shell: true });
      let infoJson = '';
      let errorMessage = '';
      mediainfo.stdout.setEncoding('utf8');
      mediainfo.stdout.on('data', (data) => {
        infoJson += data;
      });
      mediainfo.stderr.setEncoding('utf8');
      mediainfo.stderr.on('data', (data) => {
        errorMessage += data + '\n';
      });

      mediainfo.on('exit', (code: number) => {
        if (code !== 0) {
          reject({ code: code, message: errorMessage });
        } else {
          const fileData = JSON.parse(infoJson);
          resolve(fileData);
        }
      });
    });
  }

  createH264Params(encodedLibrarySettings: string, sameRes: boolean = false) {
    if (!encodedLibrarySettings) return '';
    const settingList = encodedLibrarySettings.replace(/:/g, '\\:').split(' / ');
    const encodingSettings = sameRes ? this.knwonEncodingSettings : this.multiResEncodingSettings;
    const filteredList = settingList.filter(value => {
      const key = value.split('=')[0];
      if (encodingSettings.indexOf(key) > -1)
        return true
      return false;
    });
    return filteredList.join(':');
  }

  isHDRVideo(colorSpace: string, colorTransfer: string, colorPrimaries: string) {
    if (colorSpace === 'bt2020nc' && colorTransfer === 'smpte2084' && colorPrimaries === 'bt2020')
      return true;
    return false;
  }

  getVideoFrameRate(avgFrameRate: string, rFrameRate: string, mediainfoFrameRate: string) {
    if (avgFrameRate) {
      if (avgFrameRate.includes('/')) {
        return Math.ceil(stringHelper.divideFromString(avgFrameRate));
      }
      return Math.ceil(+avgFrameRate);
    }
    if (rFrameRate) {
      if (rFrameRate.includes('/')) {
        return Math.ceil(stringHelper.divideFromString(rFrameRate));
      }
      return Math.ceil(+rFrameRate);
    }
    if (mediainfoFrameRate) {
      return Math.ceil(+mediainfoFrameRate);
    }
  }
}

export const mediaInfoHelper = new MediaInfoHelper();

export interface MediaInfoResult {
  creatingLibrary: CreatingLibraryInfo;
  media: MediaInfoData;
}

export interface MediaInfoData {
  '@ref': string;
  track: TrackInfo[];
}

export interface TrackInfo {
  '@type': 'General' | 'Video' | 'Audio' | 'Text';
  VideoCount?: string;
  AudioCount?: string;
  FileExtension?: string;
  Format: string;
  Format_Profile?: string;
  CodecID: string;
  CodecID_Compatible?: string;
  FileSize?: string;
  Duration: string;
  OverallBitRate?: string;
  FrameRate: string;
  FrameCount: string;
  StreamSize: string;
  HeaderSize?: string;
  DataSize?: string;
  FooterSize?: string;
  IsStreamable?: string;
  File_Created_Date?: string;
  File_Created_Date_Local?: string;
  File_Modified_Date?: string;
  File_Modified_Date_Local?: string;
  Encoded_Application?: string;
  StreamOrder?: string;
  ID?: string;
  Format_Level?: string;
  Format_Settings_CABAC?: string;
  Format_Settings_RefFrames?: string;
  BitRate?: string;
  BitRate_Maximum?: string;
  Width?: string;
  Height?: string;
  Stored_Height?: string;
  Sampled_Width?: string;
  Sampled_Height?: string;
  PixelAspectRatio?: string;
  DisplayAspectRatio?: string;
  Rotation?: string;
  FrameRate_Mode?: string;
  FrameRate_Mode_Original?: string;
  ColorSpace?: string;
  ChromaSubsampling?: string;
  BitDepth?: string;
  ScanType?: string;
  Title?: string;
  Encoded_Library?: string;
  Encoded_Library_Name?: string;
  Encoded_Library_Version?: string;
  Encoded_Library_Settings?: string;
  Language?: string;
  extra?: ExtraInfo;
  Format_AdditionalFeatures?: string;
  Source_Duration?: string;
  BitRate_Mode?: string;
  Channels?: string;
  ChannelPositions?: string;
  ChannelLayout?: string;
  SamplesPerFrame?: string;
  SamplingRate?: string;
  SamplingCount?: string;
  Source_FrameCount?: string;
  Compression_Mode?: string;
  StreamSize_Proportion?: string;
  Source_StreamSize?: string;
  Source_StreamSize_Proportion?: string;
  Default?: string;
  AlternateGroup?: string;
}

interface ExtraInfo {
  CodecConfigurationBox?: string;
  Source_Delay?: string;
  Source_Delay_Source?: string;
}

interface CreatingLibraryInfo {
  name: string;
  version: string;
  url: string;
}