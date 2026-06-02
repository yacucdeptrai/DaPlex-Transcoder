import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import path from 'path';

import { AudioCodec, VideoCodec } from '../../enums';
import {
  FFMPEG_RECONNECT_ARGS,
  HDR_TONEMAP_FILTER,
  MAX_AUDIO_CHANNELS,
  OPUS_STEREO_BITRATE,
  OPUS_SURROUND_BITRATE_PER_CHANNEL,
  SPLIT_SEGMENT_FOLDER,
  THUMBNAIL_FOLDER
} from '../../config';
import { ffmpegHelper, mediaInfoHelper } from '../../utils';
import {
  AdvancedVideoSettings,
  CreateAudioEncodingArgsOptions,
  CreateVideoEncodingArgsOptions,
  IEncodingSetting,
  ResolveVideoFiltersOptions,
  VideoSourceInfo
} from './interfaces';

/**
 * Builds the ffmpeg / MP4Box / rclone command-line argument arrays used by the
 * transcode pipeline. Extracted verbatim from VideoService (Phase 7.5) so the
 * orchestrator no longer owns ~330 lines of pure argument construction.
 *
 * Reads the same config keys VideoService does (USE_URL_INPUT, SVT_AV1_PRESET,
 * RCLONE_CONFIG_FILE) in its own constructor — no shared mutable state.
 */
@Injectable()
export class EncodingArgsService {
  private UseURLInput: boolean;
  private thumbnailFolder: string;

  constructor(private configService: ConfigService) {
    this.UseURLInput = this.configService.get<string>('USE_URL_INPUT') === 'true';
    this.thumbnailFolder = THUMBNAIL_FOLDER;
  }

  createAudioEncodingArgs(options: CreateAudioEncodingArgsOptions) {
    const { inputFile, parsedInput, audioParams, codec, channels, downmix, audioIndex, outputFileName } = options;
    const bitrate =
      AudioCodec.OPUS === codec
        ? OPUS_STEREO_BITRATE
        : AudioCodec.OPUS_SURROUND === codec
        ? OPUS_SURROUND_BITRATE_PER_CHANNEL * channels
        : 0;
    const args: string[] = [
      '-hide_banner',
      '-y',
      '-progress',
      'pipe:1',
      '-loglevel',
      'error',
      '-i',
      `"${inputFile}"`,
      '-vn'
    ];
    if (this.UseURLInput) {
      args.push(...FFMPEG_RECONNECT_ARGS);
    }
    if (bitrate > 0) {
      args.push('-b:a', `${bitrate}K`);
    }
    args.push(...audioParams);
    if (downmix) {
      if (codec === AudioCodec.AAC) {
        args.push(
          '-af',
          '"lowpass=c=LFE:f=120,pan=stereo|FL=.3FL+.21FC+.3FLC+.21SL+.21BL+.15BC+.21LFE|FR=.3FR+.21FC+.3FRC+.21SR+.21BR+.15BC+.21LFE,volume=1.6"'
        );
      } else if (codec === AudioCodec.OPUS) {
        args.push('-ac', '2');
        args.push('-mapping_family', '0');
      }
    } else if (channels > 2) {
      const channelValue = channels <= MAX_AUDIO_CHANNELS ? channels.toString() : MAX_AUDIO_CHANNELS.toString();
      args.push('-ac', channelValue);
      if (codec === AudioCodec.OPUS_SURROUND) {
        args.push('-mapping_family', '1');
      }
    }
    args.push(
      '-map',
      `0:${audioIndex}`,
      //'-map_metadata', '-1',
      '-map_chapters',
      '-1',
      '-f',
      'mp4',
      `"${parsedInput.dir}/${outputFileName}"`
    );
    return args;
  }

  createVideoEncodingArgs(options: CreateVideoEncodingArgsOptions) {
    const {
      inputFile,
      parsedInput,
      codec,
      quality,
      videoParams,
      sourceInfo,
      crfKey,
      advancedSettings,
      encodingSetting,
      splitFrom,
      splitDuration,
      outputFileName
    } = options;
    const gopSize = (sourceInfo.fps ? sourceInfo.fps * 2 : 48).toString();
    const bitDepth = codec === VideoCodec.H264 ? 8 : 10;
    const videoFilters = this.resolveVideoFilters({
      quality,
      hdrTonemap: codec === VideoCodec.H264 && sourceInfo.isHDR,
      bitDepth
    });
    const args: string[] = ['-hide_banner', '-y', '-hwaccel', 'auto', '-progress', 'pipe:1', '-loglevel', 'error'];
    if (this.UseURLInput) {
      args.push(...FFMPEG_RECONNECT_ARGS);
    }
    splitFrom && args.push('-ss', splitFrom);
    args.push('-i', `"${inputFile}"`);
    splitDuration && args.push('-t', splitDuration);
    args.push(...videoParams, '-g', gopSize, '-keyint_min', gopSize, '-sc_threshold', '0');
    if (encodingSetting) this.resolveEncodingSettings(args, encodingSetting, sourceInfo, crfKey);
    if (codec === VideoCodec.H264) this.resolveH264Params(args, advancedSettings, quality, sourceInfo);
    else if (codec === VideoCodec.AV1) this.resolveSVTAV1Params(args, advancedSettings, sourceInfo);
    args.push(
      '-map',
      '0:v:0',
      //'-map_metadata', '-1',
      '-map_chapters',
      '-1',
      '-vf',
      videoFilters,
      //'-movflags', '+faststart',
      `"${parsedInput.dir}/${outputFileName}"`
    );
    return args;
  }

  createTwoPassesVideoEncodingArgs(options: CreateVideoEncodingArgsOptions & { pass: number }) {
    const {
      inputFile,
      parsedInput,
      codec,
      quality,
      videoParams,
      sourceInfo,
      crfKey,
      advancedSettings,
      encodingSetting,
      pass,
      splitFrom,
      splitDuration,
      segmentIndex,
      outputFileName
    } = options;
    const gopSize = (sourceInfo.fps ? sourceInfo.fps * 2 : 48).toString();
    const bitDepth = codec === VideoCodec.H264 ? 8 : 10;
    const videoFilters = this.resolveVideoFilters({ quality, hdrTonemap: false, bitDepth });
    // Both passes share an identical prefix; only the stream mapping and the
    // trailing -pass directive differ. Build the common args once.
    const args = ['-hide_banner', '-y', '-hwaccel', 'auto', '-progress', 'pipe:1', '-loglevel', 'error'];
    if (this.UseURLInput) {
      args.push(...FFMPEG_RECONNECT_ARGS);
    }
    splitFrom && args.push('-ss', splitFrom);
    args.push('-i', `"${inputFile}"`);
    splitDuration && args.push('-t', splitDuration);
    args.push(...videoParams, '-g', gopSize, '-keyint_min', gopSize, '-sc_threshold', '0');
    if (encodingSetting) this.resolveEncodingSettings(args, encodingSetting, sourceInfo, crfKey);
    if (codec === VideoCodec.H264) this.resolveH264Params(args, advancedSettings, quality, sourceInfo);
    else if (codec === VideoCodec.AV1) this.resolveSVTAV1Params(args, advancedSettings, sourceInfo);

    // Stream mapping: the final pass (2) also strips chapters before muxing output.
    args.push('-map', '0:v:0');
    if (pass === 2) args.push('-map_chapters', '-1');
    args.push('-vf', videoFilters);
    //'-movflags', '+faststart'

    // Shared two-pass log file (segmented encodes write under the split folder).
    const passLogDir = segmentIndex != null ? `${parsedInput.dir}/${SPLIT_SEGMENT_FOLDER}` : parsedInput.dir;
    args.push('-passlogfile', `"${passLogDir}/${parsedInput.name}_2pass.log"`);

    // Pass-specific tail: pass 1 analyzes to a null sink, pass 2 writes the output.
    if (pass === 1) {
      const nullSink = process.platform === 'win32' ? 'NUL' : '/dev/null';
      args.push('-pass', '1', '-an', '-f', 'null', nullSink);
    } else {
      args.push('-pass', '2', `"${parsedInput.dir}/${outputFileName}"`);
    }
    return args;
  }

  private resolveEncodingSettings(
    args: string[],
    encodingSetting: IEncodingSetting,
    sourceInfo: VideoSourceInfo,
    crfKey: 'crf' | 'cq' = 'crf'
  ) {
    let crfValue = null;
    if (crfKey === 'crf')
      if (sourceInfo.codec === 'h265') crfValue = encodingSetting.h265Crf;
      else crfValue = encodingSetting.crf;
    else if (crfKey === 'cq') crfValue = encodingSetting.cq;
    crfValue && args.push('-crf', crfValue.toString());
    // Should double the bitrate when the source codec isn't h264 (could be h265, vp9 or av1)
    const baseBitrate = sourceInfo.codec === 'h264' ? sourceInfo.bitrate : sourceInfo.bitrate * 2;
    if (encodingSetting.useLowerRate && baseBitrate > 0 && baseBitrate < encodingSetting.maxrate) {
      encodingSetting.maxrate && args.push('-maxrate', `${baseBitrate}K`);
      encodingSetting.bufsize && args.push('-bufsize', `${baseBitrate * 2}K`);
    } else {
      encodingSetting.maxrate && args.push('-maxrate', `${encodingSetting.maxrate}K`);
      encodingSetting.bufsize && args.push('-bufsize', `${encodingSetting.bufsize}K`);
    }
  }

  private resolveH264Params(
    args: string[],
    advancedSettings: AdvancedVideoSettings,
    quality: number,
    sourceInfo: VideoSourceInfo
  ) {
    if (advancedSettings.h264Tune) {
      args.push('-tune', advancedSettings.h264Tune);
    }
    if (quality >= 1440) {
      // Find the best h264 profile level for > 2k resolution
      const level = ffmpegHelper.findH264ProfileLevel(sourceInfo.width, sourceInfo.height, quality, sourceInfo.fps);
      if (level !== null) {
        args.push('-level:v', level);
      }
    }
    if (sourceInfo.sourceH264Params) {
      const x264Params = mediaInfoHelper.createH264Params(sourceInfo.sourceH264Params, sourceInfo.height === quality);
      args.push('-x264-params', `"${x264Params}"`);
    }
  }

  private resolveSVTAV1Params(args: string[], advancedSettings: AdvancedVideoSettings, sourceInfo: VideoSourceInfo) {
    const svtAv1Preset = this.configService.get<string>('SVT_AV1_PRESET');
    const svtAV1PresetParams = {
      main: [
        'tune=0',
        'enable-overlays=1',
        'film-grain=0',
        'film-grain-denoise=0',
        'scd=1',
        'sharpness=0',
        'enable-qm=1',
        'qm-min=0',
        'enable-variance-boost=1'
      ],
      psy: ['tune=0', 'enable-overlays=1', 'film-grain=0', 'film-grain-denoise=0', 'sharpness=0', 'scd=1'],
      hdr: ['sharpness=0']
    };
    const svtAV1Params =
      svtAv1Preset === 'psy'
        ? svtAV1PresetParams.psy
        : svtAv1Preset === 'hdr'
        ? svtAV1PresetParams.hdr
        : svtAV1PresetParams.main;
    if (advancedSettings.h264Tune !== 'animation') svtAV1Params.push('scm=0');
    if (sourceInfo.hdrParams) {
      args.push(...sourceInfo.hdrParams.ffmpegParams);
      svtAV1Params.push(sourceInfo.hdrParams.libsvtav1Params);
    } else {
      svtAV1Params.push('luminance-qp-bias=30');
    }
    const gopSize = (sourceInfo.fps ? sourceInfo.fps * 2 : 48).toString();
    svtAV1Params.push(`keyint=${gopSize}`);
    args.push('-svtav1-params', `"${svtAV1Params.join(':')}"`);
  }

  private resolveVideoFilters(options: ResolveVideoFiltersOptions) {
    const videoFilters: string[] = [];
    if (options.quality) {
      videoFilters.push(`scale=-2:${options.quality}`);
    }
    if (options.hdrTonemap) {
      videoFilters.push(HDR_TONEMAP_FILTER);
      if (options.bitDepth === 10) {
        videoFilters.push('format=yuv420p10le');
      } else {
        videoFilters.push('format=yuv420p');
      }
    }
    return videoFilters.join(',');
  }

  createConcatSegmentArgs(inputFile: string, parsedInput: path.ParsedPath, outputFile: string) {
    const args = [
      '-hide_banner',
      '-y',
      '-progress',
      'pipe:1',
      '-loglevel',
      'error',
      '-f',
      'concat',
      '-safe',
      '0',
      '-i',
      `"${inputFile}"`,
      '-c',
      'copy',
      `"${parsedInput.dir}/${outputFile}"`
    ];
    return args;
  }

  createMP4BoxPackArgs(input: string, parsedInput: path.ParsedPath, tempFileName: string, playlistName: string) {
    const segmentInitName = process.platform === 'win32' ? '$Init=$' : '\\$Init=\\$';
    const args: string[] = [
      '-dash',
      '6000',
      '-profile',
      'onDemand',
      '-segment-name',
      `"${tempFileName}${segmentInitName}"`,
      '-out',
      `"${parsedInput.dir}/${playlistName}:dual"`,
      `"${input}"`
    ];
    return args;
  }

  createRcloneMoveArgs(source: string, dest: string, include?: string) {
    const rcloneConfigFile = this.configService.get<string>('RCLONE_CONFIG_FILE');
    const args: string[] = [
      '--config',
      rcloneConfigFile,
      '--low-level-retries',
      '5',
      '-v',
      '--use-json-log',
      '--stats',
      '3s',
      'move',
      `"${source}"`,
      `"${dest}"`
    ];
    if (include) {
      args.push('--include', include);
    }
    return args;
  }

  createRcloneMoveThumbArgs(transcodeDir: string, remote: string, parentFolder: string, sync: boolean = false) {
    const targetCommand = sync ? 'sync' : 'move';
    const rcloneConfigFile = this.configService.get<string>('RCLONE_CONFIG_FILE');
    const args: string[] = [
      '--config',
      rcloneConfigFile,
      '--low-level-retries',
      '5',
      '-v',
      '--use-json-log',
      '--stats',
      '3s',
      targetCommand,
      `"${transcodeDir}/${this.thumbnailFolder}"`,
      `"${remote}:${parentFolder}/${this.thumbnailFolder}"`
    ];
    return args;
  }
}
