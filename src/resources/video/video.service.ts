import { Inject, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';
import { InjectQueue } from '@nestjs/bullmq';
import { Job, Queue, UnrecoverableError } from 'bullmq';
import mongoose from 'mongoose';
import path from 'path';
import FFprobe from 'ffprobe-client';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';

import { externalStorageModel } from '../../models/external-storage.model';
import { mediaStorageModel } from '../../models/media-storage.model';
import { settingModel } from '../../models/setting.model';
import { mediaModel } from '../../models/media.model';
import {
  IVideoData,
  IJobData,
  IEncodingSetting,
  MediaQueueResult,
  EncodeAudioOptions,
  EncodeVideoOptions,
  VideoSourceInfo,
  EncodeAudioByTrackOptions,
  ValidateSourceQualityOptions
} from './interfaces';
import { EncodingArgsService } from './encoding-args.service';
import { QualityResolverService } from './quality-resolver.service';
import { ProcessSpawnerService } from './process-spawner.service';
import { RcloneService } from './rclone.service';
import { AudioCodec, StatusCode, VideoCodec, RejectCode, TaskQueue } from '../../enums';
import {
  ENCODING_QUALITY,
  AUDIO_PARAMS,
  AUDIO_SURROUND_PARAMS,
  VIDEO_H264_PARAMS,
  VIDEO_H265_PARAMS,
  VIDEO_VP9_PARAMS,
  VIDEO_AV1_PARAMS,
  AUDIO_SPEED_PARAMS,
  AUDIO_SURROUND_OPUS_PARAMS,
  NEXT_GEN_ENCODING_QUALITY,
  SPLIT_SEGMENT_FOLDER,
  CONCAT_SEGMENT_FILE,
  THUMBNAIL_FOLDER,
  EXPECTED_AUDIO_STREAMS,
  SURROUND_CHANNEL_COUNTS
} from '../../config';
import { DaplexApiService } from '../../common/modules/daplex-api';
import { TranscoderApiService } from '../../common/modules/transcoder-api';
import {
  createSnowFlakeId,
  diskSpaceUtil,
  fileHelper,
  generateSprites,
  hdrMetadataHelper,
  mediaInfoHelper,
  MediaInfoResult,
  stringHelper,
  StreamManifest,
  rcloneHelper,
  videoSourceHelper
} from '../../utils';

type JobNameType =
  | 'update-source'
  | 'add-stream-video'
  | 'add-stream-audio'
  | 'add-stream-manifest'
  | 'finished-encoding'
  | 'cancelled-encoding'
  | 'retry-encoding'
  | 'failed-encoding';

@Injectable()
export class VideoService {
  private AudioParams: string[];
  private AudioSpeedParams: string[];
  private AudioSurroundParams: string[];
  private AudioSurroundOpusParams: string[];
  private VideoH264Params: string[];
  private VideoH265Params: string[];
  private VideoVP9Params: string[];
  private VideoAV1Params: string[];
  private UseURLInput: boolean;
  private SplitEncoding: boolean;
  private CanceledJobIds: (string | number)[];
  private RetryEncoding: boolean;
  private CanRetryEncoding: boolean;
  private TranscoderPriority: number;
  private thumbnailFolder: string;

  constructor(
    @Inject(WINSTON_MODULE_PROVIDER) private readonly logger: Logger,
    @InjectQueue(TaskQueue.VIDEO_TRANSCODE_RESULT)
    private videoResultQueue: Queue<MediaQueueResult, Record<string, never>, JobNameType>,
    private configService: ConfigService,
    private daplexApiService: DaplexApiService,
    private transcoderApiService: TranscoderApiService,
    private encodingArgs: EncodingArgsService,
    private qualityResolver: QualityResolverService,
    private spawner: ProcessSpawnerService,
    private rclone: RcloneService
  ) {
    const audioParams = this.configService.get<string>('AUDIO_PARAMS');
    this.AudioParams = audioParams ? audioParams.split(' ') : AUDIO_PARAMS;
    const audioSpeedParams = this.configService.get<string>('AUDIO_SPEED_PARAMS');
    this.AudioSpeedParams = audioSpeedParams ? audioSpeedParams.split(' ') : AUDIO_SPEED_PARAMS;
    const audioSurroundParams = this.configService.get<string>('AUDIO_SURROUND_PARAMS');
    this.AudioSurroundParams = audioSurroundParams ? audioSurroundParams.split(' ') : AUDIO_SURROUND_PARAMS;
    const audioSurroundOpusParams = this.configService.get<string>('AUDIO_SURROUND_OPUS_PARAMS');
    this.AudioSurroundOpusParams = audioSurroundOpusParams
      ? audioSurroundOpusParams.split(' ')
      : AUDIO_SURROUND_OPUS_PARAMS;
    const videoH264Params = this.configService.get<string>('VIDEO_H264_PARAMS');
    this.VideoH264Params = videoH264Params ? videoH264Params.split(' ') : VIDEO_H264_PARAMS;
    const videoH265Params = this.configService.get<string>('VIDEO_H265_PARAMS');
    this.VideoH265Params = videoH265Params ? videoH265Params.split(' ') : VIDEO_H265_PARAMS;
    const videoVP9Params = this.configService.get<string>('VIDEO_VP9_PARAMS');
    this.VideoVP9Params = videoVP9Params ? videoVP9Params.split(' ') : VIDEO_VP9_PARAMS;
    const videoAV1Params = this.configService.get<string>('VIDEO_AV1_PARAMS');
    this.VideoAV1Params = videoAV1Params ? videoAV1Params.split(' ') : VIDEO_AV1_PARAMS;
    this.UseURLInput = this.configService.get<string>('USE_URL_INPUT') === 'true';
    this.SplitEncoding = this.configService.get<string>('SPLIT_ENCODING') === 'true';
    this.CanceledJobIds = [];
    this.RetryEncoding = false;
    this.CanRetryEncoding = false;
    this.TranscoderPriority = 0;
    this.thumbnailFolder = THUMBNAIL_FOLDER;
    this.spawner.setStateAccessors({
      getCanceledJobIds: () => this.CanceledJobIds,
      setCanceledJobIds: (ids) => (this.CanceledJobIds = ids),
      getRetryEncoding: () => this.RetryEncoding,
      setRetryEncoding: (value) => (this.RetryEncoding = value),
      getCanRetryEncoding: () => this.CanRetryEncoding
    });
  }

  async transcode(job: Job<IVideoData>, codec: VideoCodec = VideoCodec.H264) {
    const cancelIndex = this.CanceledJobIds.findIndex((j) => +j === +job.id);
    if (cancelIndex > -1) {
      this.CanceledJobIds = this.CanceledJobIds.filter((id) => +id > +job.id);
      this.logger.info(`Received cancel signal from job id: ${job.id}`);
      return {};
    }

    // Connect to MongoDB
    await mongoose.connect(this.configService.get<string>('DATABASE_URL'), { family: 4, useBigInt64: true });
    const appSettings = await settingModel.findOne({}).lean().exec();
    const mediaInfo = await mediaModel
      .findOne({ _id: BigInt(job.data.media) }, { _id: 1, originalLang: 1 })
      .lean()
      .exec();
    const streamStorage = await externalStorageModel
      .findOne({ _id: BigInt(job.data.storage) }, { _id: 1, publicUrl: 1 })
      .lean()
      .exec();

    const audioParams = appSettings.audioParams ? appSettings.audioParams.split(' ') : this.AudioParams;
    const audioSpeedParams = appSettings.audioSpeedParams
      ? appSettings.audioSpeedParams.split(' ')
      : this.AudioSpeedParams;
    const audioSurroundParams = appSettings.audioSurroundParams
      ? appSettings.audioSurroundParams.split(' ')
      : this.AudioSurroundParams;
    const audioSurroundOpusParams = appSettings.audioSurroundOpusParams
      ? appSettings.audioSurroundOpusParams.split(' ')
      : this.AudioSurroundOpusParams;
    const videoH264Params = appSettings.videoH264Params ? appSettings.videoH264Params.split(' ') : this.VideoH264Params;
    const videoH265Params = appSettings.videoH265Params ? appSettings.videoH265Params.split(' ') : this.VideoH265Params;
    const videoVP9Params = appSettings.videoVP9Params ? appSettings.videoVP9Params.split(' ') : this.VideoVP9Params;
    const videoAV1Params = appSettings.videoAV1Params ? appSettings.videoAV1Params.split(' ') : this.VideoAV1Params;
    const qualityList =
      VideoCodec.H264 === codec
        ? Array.isArray(appSettings.videoQualityList) && appSettings.videoQualityList.length
          ? appSettings.videoQualityList
          : ENCODING_QUALITY
        : Array.isArray(appSettings.videoNextGenQualityList) && appSettings.videoNextGenQualityList.length
        ? appSettings.videoNextGenQualityList
        : NEXT_GEN_ENCODING_QUALITY;
    const encodingSettings = appSettings.videoEncodingSettings || [];

    const rcloneDir = this.configService.get<string>('RCLONE_DIR');
    const rcloneConfigFile = this.configService.get<string>('RCLONE_CONFIG_FILE');
    const transcodeDir = `${this.configService.get<string>('TRANSCODE_DIR')}/${job.id}`;
    const ffmpegDir = this.configService.get<string>('FFMPEG_DIR');
    const mediainfoDir = this.configService.get<string>('MEDIAINFO_DIR');
    const trimmedFileName = job.data.linkedStorage
      ? stringHelper.trimSlugFilename(job.data.filename)
      : job.data.filename; // Trim saved file name
    const inputFile = `${transcodeDir}/${trimmedFileName}`;
    const parsedInput = path.parse(inputFile);

    await this.ensureRcloneConfigExist(rcloneConfigFile, job.data.storage, job);
    if (job.data.linkedStorage) await this.ensureRcloneConfigExist(rcloneConfigFile, job.data.linkedStorage, job);

    let linkedInputUrl = this.UseURLInput ? await this.getLinkedSourceUrl(job) : null;

    // Retry if the transcoder was interrupted before
    const retryFromInterruption = await fileHelper.fileExists(transcodeDir);
    if (retryFromInterruption) {
      this.logger.notice(
        'Transcode directory detected, maybe the transcoder was not exited properly before, cleaning up...'
      );
      const status = { jobId: job.id, ...job.data };
      await this.videoResultQueue.add('retry-encoding', status);
      await fileHelper.deleteFolder(transcodeDir);
    }

    let availableQualityList: number[] | null = null;
    const forcedQualityList = job.data.advancedOptions?.forceVideoQuality || [];
    // Find and validate source quality if the quality is available on db
    {
      const sourceInfo = await mediaStorageModel
        .findOne({ _id: BigInt(job.data._id) }, { _id: 1, name: 1, quality: 1 })
        .lean()
        .exec();
      if (sourceInfo?.quality) {
        try {
          availableQualityList = await this.validateSourceQuality({
            parsedInput,
            quality: sourceInfo.quality,
            qualityList,
            forcedQualityList,
            fallbackQualityList: [Math.min(...qualityList)],
            codec,
            retryFromInterruption,
            job
          });
          if (availableQualityList === null) return {}; // There's nothing to encode
        } finally {
          if (availableQualityList === null) await fileHelper.deleteFolder(transcodeDir);
        }
      }
    }

    // Disconnect MongoDB
    await mongoose.disconnect();

    await fileHelper.createDir(transcodeDir);
    // Still need to download for audio encoding
    if (!this.UseURLInput || codec === VideoCodec.H264) {
      this.logger.info(`Downloading file from media id: ${job.data._id}`);
      try {
        const downloadedFileStats = await fileHelper.statFile(inputFile);
        if (!downloadedFileStats || downloadedFileStats.size !== job.data.size) {
          if (downloadedFileStats) await fileHelper.deleteFile(inputFile); // Delete file if exist
          const downloadStorage = job.data.linkedStorage || job.data.storage;
          await rcloneHelper.downloadFile(
            rcloneConfigFile,
            rcloneDir,
            downloadStorage,
            job.data.path,
            job.data.filename,
            transcodeDir,
            !!job.data.linkedStorage,
            (args) => {
              this.logger.info('rclone ' + args.join(' '));
            }
          );
          const postDownloadStats = await fileHelper.statFile(inputFile);
          if (!postDownloadStats || postDownloadStats.size === 0) {
            throw new Error(
              `Download produced no file or empty file at ${inputFile} (remote: ${downloadStorage}:${job.data.path}/${job.data.filename})`
            );
          }
          if (job.data.linkedStorage) {
            // Trim file name and create folder on remote
            await Promise.all([
              fileHelper.renameFile(`${transcodeDir}/${job.data.filename}`, inputFile),
              rcloneHelper.mkdirRemote(rcloneConfigFile, rcloneDir, job.data.storage, job.data._id)
            ]);
          }
        }
      } catch (e) {
        console.error(e);
        this.logger.error(e);
        await fileHelper.deleteFolder(transcodeDir);
        const statusError = await this.generateStatusError(StatusCode.DOWNLOAD_FAILED, job);
        throw new Error(statusError.errorCode);
      }
    }

    let videoInfo: FFprobe.FFProbeResult;
    let videoMIInfo: MediaInfoResult;
    try {
      if (!this.UseURLInput) {
        const probeTarget = inputFile;
        const probeStats = await fileHelper.statFile(probeTarget);
        if (!probeStats) {
          throw new Error(`Input file does not exist: ${probeTarget}`);
        }
        this.logger.info(`Processing input file: ${probeTarget} (${probeStats.size} bytes)`);
        videoInfo = await FFprobe(probeTarget, { path: `${ffmpegDir}/ffprobe` });
        videoMIInfo = await mediaInfoHelper.getMediaInfo(probeTarget, mediainfoDir);
      } else {
        this.logger.info(`Processing input file: ${linkedInputUrl}`);
        videoInfo = await FFprobe(linkedInputUrl, { path: `${ffmpegDir}/ffprobe` });
        videoMIInfo = await mediaInfoHelper.getMediaInfo(linkedInputUrl, mediainfoDir);
      }
    } catch (e) {
      const errMsg = e instanceof Error ? e.message : String(e);
      this.logger.error(`PROBE_FAILED: ${errMsg}`);
      console.error(e);
      await fileHelper.deleteFolder(transcodeDir);
      const statusError = await this.generateStatusError(StatusCode.PROBE_FAILED, job, { discard: true });
      throw new UnrecoverableError(statusError.errorCode);
    }

    const videoTrack = videoInfo.streams.find((s) => s.codec_type === 'video');
    const videoMITrack = videoMIInfo.media.track.find((s) => s['@type'] === 'Video');
    if (!videoTrack || !videoMITrack) {
      this.logger.error('Video track not found');
      await fileHelper.deleteFolder(transcodeDir);
      const statusError = await this.generateStatusError(StatusCode.NO_VIDEO_TRACK, job, { discard: true });
      throw new UnrecoverableError(statusError.errorCode);
    }

    const audioTracks = videoInfo.streams.filter((s) => s.codec_type === 'audio');
    if (!audioTracks.length) {
      this.logger.error('Audio track not found');
      await fileHelper.deleteFolder(transcodeDir);
      const statusError = await this.generateStatusError(StatusCode.NO_AUDIO_TRACK, job, { discard: true });
      throw new UnrecoverableError(statusError.errorCode);
    }

    const runtime = videoInfo.format.duration ? Math.trunc(+videoInfo.format.duration) : 0;
    const videoDuration = videoTrack.duration ? Math.trunc(+videoTrack.duration) : runtime;
    const videoFps = mediaInfoHelper.getVideoFrameRate(
      videoTrack.avg_frame_rate,
      videoTrack.r_frame_rate,
      videoMITrack.FrameRate
    );
    const videoBitrate = videoTrack.bit_rate
      ? Math.round(+videoTrack.bit_rate / 1000)
      : videoMITrack.BitRate
      ? Math.round(+videoMITrack.BitRate / 1000)
      : 0; // Bitrate in Kbps
    const videoCodec = videoTrack.codec_name || '';
    const videoSourceH264Params =
      videoCodec === 'h264' && videoMITrack.Encoded_Library_Settings ? videoMITrack.Encoded_Library_Settings : '';

    // Validate source file by reading the local file
    if (!availableQualityList) {
      try {
        availableQualityList = await this.validateSourceQuality({
          parsedInput,
          quality: videoTrack.height,
          qualityList,
          forcedQualityList,
          fallbackQualityList: [Math.min(...qualityList)],
          codec,
          retryFromInterruption,
          job
        });
        if (availableQualityList === null) return {}; // There's nothing to encode
      } finally {
        if (availableQualityList === null) await fileHelper.deleteFolder(transcodeDir);
      }
    }

    const srcWidth = videoTrack.width || 0;
    const srcHeight = videoTrack.height || 0;

    this.logger.info(`Video resolution: ${srcWidth}x${srcHeight}`);

    await this.daplexApiService.ensureProducerAppIsOnline(job.data.producerUrl);
    await this.videoResultQueue.add('update-source', {
      ...job.data,
      jobId: job.id,
      progress: {
        sourceId: job.data._id,
        quality: srcHeight,
        runtime: runtime
      }
    });

    const manifest = new StreamManifest();
    // Load manifest if encode audio or video only
    if (job.data.advancedOptions?.audioOnly || job.data.advancedOptions?.videoOnly) {
      const existingManifestData = await this.qualityResolver.findExistingManifest(
        job.data.storage,
        job.data._id,
        codec
      );
      if (existingManifestData !== null) {
        manifest.load(existingManifestData);
        job.data.advancedOptions?.audioOnly && manifest.clearTracks('audio');
        job.data.advancedOptions?.videoOnly && manifest.clearTracks('video');
      }
    }

    // Skip audio encoding for other codecs
    // Only encode if there's no audio track inside the manifest data
    if (
      codec === VideoCodec.H264 &&
      !job.data.advancedOptions?.videoOnly /*&& manifest.manifest.audioTracks.length === 0*/
    ) {
      this.logger.info('Processing audio');
      const defaultAudioTrack = audioTracks.find((a) => a.disposition.default) || audioTracks[0];
      const allowedAudioTracks = new Set(job.data.advancedOptions?.selectAudioTracks || []);
      if (allowedAudioTracks.size === 0) allowedAudioTracks.add(defaultAudioTrack.index);

      const audioNormalTrack = audioTracks.find((a) => a.channels <= 2 && allowedAudioTracks.has(a.index));
      const audioSurroundTrack = audioTracks.find((a) => a.channels > 2 && allowedAudioTracks.has(a.index));
      const audioPrimaryTracks = [audioNormalTrack, audioSurroundTrack].filter((a) => a != null);
      const allowedExtraAudioTracks = new Set(job.data.advancedOptions?.extraAudioTracks || []);
      const audioExtraTracks = audioTracks.filter(
        (a) => !audioPrimaryTracks.includes(a) && allowedExtraAudioTracks.has(a.index)
      );

      const firstAudioTrack = audioNormalTrack || audioSurroundTrack || defaultAudioTrack;
      const secondAudioTrack = audioSurroundTrack;

      try {
        // Audio language for primary track
        const audioOriginalLang = mediaInfo.originalLang;
        // Encode surround audio track
        if (secondAudioTrack != null) {
          this.logger.info(`Audio track index ${secondAudioTrack.index} (surround)`);
          await this.encodeAudioByTrack({
            inputFile,
            parsedInput,
            type: 'surround',
            audioTrack: secondAudioTrack,
            audioAACParams: audioSurroundParams,
            audioOpusParams: audioSurroundOpusParams,
            isDefault: true,
            downmix: false,
            language: audioOriginalLang,
            manifest,
            job
          });
        }
        // Encode stereo or mono audio track
        this.logger.info(`Audio track index ${firstAudioTrack.index} (normal)`);
        await this.encodeAudioByTrack({
          inputFile,
          parsedInput,
          type: 'normal',
          audioTrack: firstAudioTrack,
          audioAACParams: audioParams,
          audioOpusParams: audioSpeedParams,
          isDefault: !secondAudioTrack,
          downmix: firstAudioTrack.channels > 2,
          language: audioOriginalLang,
          manifest,
          job
        });
        // Encode any others audio tracks
        for (let i = 0; i < audioExtraTracks.length; i++) {
          const extraAudioTrack = audioExtraTracks[i];
          const extraTrackLang = extraAudioTrack.tags?.language || 'N/A';
          const extraTrackType = extraAudioTrack.channels > 2 ? 'surround' : 'normal';
          const extraAACParams = extraAudioTrack.channels > 2 ? audioSurroundParams : audioParams;
          const extraOpusParams = extraAudioTrack.channels > 2 ? audioSurroundOpusParams : audioSpeedParams;
          this.logger.info(
            `Audio track index ${extraAudioTrack.index} (others, channels: ${extraAudioTrack.channels}, language: ${extraTrackLang})`
          );
          await this.encodeAudioByTrack({
            inputFile,
            parsedInput,
            type: extraTrackType,
            audioTrack: extraAudioTrack,
            audioAACParams: extraAACParams,
            audioOpusParams: extraOpusParams,
            isDefault: false,
            downmix: false,
            manifest,
            job
          });
        }
        // Save and update manifest file when encoding audio only
        if (job.data.advancedOptions?.audioOnly) {
          await this.saveManifestFile(manifest, parsedInput.dir, codec, job);
        }
        // Delete the downloaded source file after the audio is processed, continue using URL
        if (this.UseURLInput) await fileHelper.deleteFile(inputFile);
      } catch (e) {
        console.error(e);
        this.logger.error(JSON.stringify(e));
        await fileHelper.deleteFolder(transcodeDir);
        if (e === RejectCode.JOB_CANCEL) {
          this.logger.info(`Received cancel signal from job id: ${job.id}`);
          return {};
        }
        const statusError = await this.generateStatusError(StatusCode.ENCODE_AUDIO_FAILED, job);
        throw new Error(statusError.errorCode);
      }
    }

    let remuxFileName: string | null = null;
    try {
      if (!job.data.advancedOptions?.audioOnly) {
        // Only remux when enable SplitEncoding, UseURLInput, source is not mp4 or mkv
        if (this.SplitEncoding && this.UseURLInput && !['.mkv'].includes(parsedInput.ext)) {
          await this.transcoderApiService.checkAndWaitForTranscoderPriority();
          this.logger.info(`Remuxing file: ${inputFile}`);
          remuxFileName = `${parsedInput.name}_remux_${codec}.mkv`;
          const remuxFilePath = `${parsedInput.dir}/${remuxFileName}`;
          const remuxUrlFolder = `${job.data.storage}:${job.data._id}`;
          const remuxUrlPath = `/${job.data._id}/${remuxFileName}`;
          await videoSourceHelper.remuxSourceMKV(linkedInputUrl, remuxFilePath, {
            ffmpegDir: ffmpegDir,
            duration: videoDuration,
            videoOnly: true,
            //audioCodec: (videoInfo.format.format_name === 'mpegts' ||
            // audioTracks[0].codec_name === 'pcm_bluray') ? 'pcm_s24le' : 'copy',
            useURLInput: this.UseURLInput,
            jobId: job.id,
            canceledJobIds: this.CanceledJobIds,
            logFn: (message) => {
              this.logger.info(message);
            }
          });
          const moveRemuxFileArgs = this.encodingArgs.createRcloneMoveArgs(remuxFilePath, remuxUrlFolder);
          await this.uploadMedia(moveRemuxFileArgs, job.id);
          linkedInputUrl = streamStorage.publicUrl.replace(':service_path', 's3').replace(':path', remuxUrlPath);
        }
        // Video info
        const isHDRVideo = mediaInfoHelper.isHDRVideo(
          videoTrack.color_space,
          videoTrack.color_transfer,
          videoTrack.color_primaries
        );
        const hdrParams =
          isHDRVideo && codec !== VideoCodec.H264
            ? await hdrMetadataHelper.getHdrMetadata(linkedInputUrl || inputFile, 0, ffmpegDir, this.logger)
            : null;
        const sourceInfo: VideoSourceInfo = {
          duration: videoDuration,
          fps: videoFps,
          bitrate: videoBitrate,
          codec: videoCodec,
          sourceH264Params: videoSourceH264Params,
          width: srcWidth,
          height: srcHeight,
          language: mediaInfo.originalLang,
          isHDR: isHDRVideo,
          hdrParams: hdrParams
        };
        if (codec === VideoCodec.H264) {
          this.logger.info('Video codec: H264');
          await this.encodeByCodec({
            inputFile,
            parsedInput,
            inputFileUrl: linkedInputUrl,
            sourceInfo,
            qualityList: availableQualityList,
            encodingSettings,
            advancedSettings: job.data.advancedOptions,
            codec: VideoCodec.H264,
            videoParams: videoH264Params,
            manifest,
            job
          });
        } else if (codec === VideoCodec.H265) {
          this.logger.info('Video codec: H265');
          await this.encodeByCodec({
            inputFile,
            parsedInput,
            inputFileUrl: linkedInputUrl,
            sourceInfo,
            qualityList: availableQualityList,
            encodingSettings,
            advancedSettings: job.data.advancedOptions,
            codec: VideoCodec.H265,
            videoParams: videoH265Params,
            manifest,
            job
          });
        } else if (codec === VideoCodec.VP9) {
          this.logger.info('Video codec: VP9');
          await this.encodeByCodec({
            inputFile,
            parsedInput,
            inputFileUrl: linkedInputUrl,
            sourceInfo,
            qualityList: availableQualityList,
            encodingSettings,
            advancedSettings: job.data.advancedOptions,
            codec: VideoCodec.VP9,
            videoParams: videoVP9Params,
            manifest,
            job
          });
        } else if (codec === VideoCodec.AV1) {
          this.logger.info('Video codec: AV1');
          await this.encodeByCodec({
            inputFile,
            parsedInput,
            inputFileUrl: linkedInputUrl,
            sourceInfo,
            qualityList: availableQualityList,
            encodingSettings,
            advancedSettings: job.data.advancedOptions,
            codec: VideoCodec.AV1,
            videoParams: videoAV1Params,
            manifest,
            job
          });
        }

        if (codec === VideoCodec.H264) {
          // Generate preview thumbnail
          this.logger.info(`Generating preview thumbnail: ${inputFile}`);
          this.setTranscoderPriority(1);
          await generateSprites(
            {
              source: linkedInputUrl || inputFile,
              output: `${parsedInput.dir}/${this.thumbnailFolder}`,
              duration: videoDuration,
              isHDR: isHDRVideo,
              ffmpegDir,
              useURLInput: this.UseURLInput,
              jobId: job.id,
              canceledJobIds: this.CanceledJobIds,
              logger: this.logger
            },
            [
              { tw: 160, th: 160, pageCols: 10, pageRows: 10, prefix: 'M', format: 'jpeg' },
              { tw: 320, th: 320, pageCols: 5, pageRows: 5, prefix: 'L', format: 'jpeg' }
            ]
          );
          this.setTranscoderPriority(0);
          const syncThumbnails = !!job.data.update;
          const rcloneMoveThumbArgs = this.encodingArgs.createRcloneMoveThumbArgs(
            transcodeDir,
            job.data.storage,
            job.data._id,
            syncThumbnails
          );
          await this.uploadMedia(rcloneMoveThumbArgs, job.id);
        }
      }

      if (job.data.replaceStreams?.length) {
        this.logger.info('Removing old streams');
        for (let i = 0; i < job.data.replaceStreams.length; i++) {
          await rcloneHelper.deletePath(
            rcloneConfigFile,
            rcloneDir,
            job.data.storage,
            `${job.data._id}/${job.data.replaceStreams[i]}`,
            (args) => {
              this.logger.info('rclone ' + args.join(' '));
            }
          );
        }
      }
      // Check uploaded files
      this.logger.info('Checking uploaded files');
      const checkFilesExclusion = `${this.thumbnailFolder}/**`;
      let uploadedFiles = await this.findUploadedFiles(job.data.storage, job.data._id, job.id, checkFilesExclusion);
      let listAttempt = 1;
      // 1 source file (0 for linked source), 3 audio files, and video files
      const expectedVideoFiles = !job.data.advancedOptions?.audioOnly ? availableQualityList.length : 0;
      const expectedAudioFiles = !job.data.advancedOptions?.videoOnly ? EXPECTED_AUDIO_STREAMS : 0;
      const totalExpectedFiles = expectedVideoFiles + (job.data.linkedStorage ? 0 : 1) + expectedAudioFiles;
      const maxTries = 5;
      while (uploadedFiles.length < totalExpectedFiles && listAttempt < maxTries) {
        uploadedFiles = await this.findUploadedFiles(job.data.storage, job.data._id, job.id, checkFilesExclusion);
        listAttempt++;
      }
      this.logger.info(`${uploadedFiles.length}/${totalExpectedFiles} files uploaded`);
    } catch (e) {
      console.error(e);
      this.logger.error(JSON.stringify(e));
      if (e === RejectCode.JOB_CANCEL) {
        this.logger.info(`Received cancel signal from job id: ${job.id}`);
        //await this.daplexApiService.ensureProducerAppIsOnline(job.data.producerUrl);
        //await this.videoResultQueue.add('cancelled-encoding', this.generateStatus(job));
        return {};
      }
      const statusError = await this.generateStatusError(StatusCode.ENCODE_VIDEO_FAILED, job);
      throw new Error(statusError.errorCode);
    } finally {
      this.logger.info('Cleaning up');
      await fileHelper.deleteFolder(transcodeDir);
      // Remove remux file if exist
      if (remuxFileName) {
        this.logger.info(`Removing remux file: ${remuxFileName}`);
        await rcloneHelper.deleteFile(
          rcloneConfigFile,
          rcloneDir,
          job.data.storage,
          `${job.data._id}/${remuxFileName}`,
          (args) => {
            this.logger.info('rclone ' + args.join(' '));
          }
        );
      }
      this.setTranscoderPriority(0);
      this.logger.info('Completed');
    }
    await this.daplexApiService.ensureProducerAppIsOnline(job.data.producerUrl);
    await this.videoResultQueue.add('finished-encoding', this.generateStatus(job));
    return {};
  }

  addToCanceled(jobData: IJobData) {
    if (jobData.id) this.CanceledJobIds.push(jobData.id);
    else if (jobData.ids) this.CanceledJobIds.push(...jobData.ids);
    return jobData;
  }

  setRetryEncoding() {
    this.RetryEncoding = true;
  }

  getTranscoderPriority() {
    return this.TranscoderPriority;
  }

  private setTranscoderPriority(priority: number) {
    this.TranscoderPriority = priority;
  }

  private async encodeAudioByTrack(options: EncodeAudioByTrackOptions) {
    const {
      inputFile,
      parsedInput,
      inputFileUrl,
      type,
      audioTrack,
      audioAACParams,
      audioOpusParams,
      isDefault,
      downmix,
      language,
      manifest,
      job
    } = options;
    const aacType = type === 'normal' ? AudioCodec.AAC : AudioCodec.AAC_SURROUND;
    const opusType = type === 'normal' ? AudioCodec.OPUS : AudioCodec.OPUS_SURROUND;
    this.logger.info('Audio codec: AAC');
    const audioDuration = audioTrack.duration ? Math.trunc(+audioTrack.duration) : 0;
    const audioChannels = audioTrack.channels || (type === 'normal' ? 2 : 0);
    const audioTitle = audioTrack.tags?.title || null;
    await this.encodeAudio({
      inputFile,
      parsedInput,
      inputFileUrl,
      sourceInfo: { duration: audioDuration, channels: audioChannels, language, title: audioTitle },
      audioTrackIndex: audioTrack.index,
      codec: aacType,
      isDefault,
      downmix,
      audioParams: audioAACParams,
      manifest,
      job
    });
    // Only encode opus surround if the source audio has 5 (4.1), 6 (5.1), 7 (6.1) or 8 (7.1) channels
    if (type === 'normal' || SURROUND_CHANNEL_COUNTS.includes(audioChannels)) {
      this.logger.info('Audio codec: OPUS');
      await this.encodeAudio({
        inputFile,
        parsedInput,
        inputFileUrl,
        sourceInfo: { duration: audioDuration, channels: audioChannels, language, title: audioTitle },
        audioTrackIndex: audioTrack.index,
        codec: opusType,
        isDefault: false,
        downmix,
        audioParams: audioOpusParams,
        manifest,
        job
      });
    }
  }

  private async encodeAudio(options: EncodeAudioOptions) {
    const {
      inputFile,
      parsedInput,
      inputFileUrl,
      sourceInfo,
      audioTrackIndex,
      codec,
      isDefault,
      downmix,
      audioParams,
      manifest,
      job
    } = options;
    const streamId = await createSnowFlakeId();

    const audioBaseName = `${parsedInput.name}_audio_${audioTrackIndex}`;
    const encodedAudioFileName = `${audioBaseName}.mp4`;
    const preparedAudioFileName = `${audioBaseName}.mp4`;
    const manifestFileName = `${audioBaseName}.m3u8`;
    const mpdManifestFileName = `${audioBaseName}.mpd`;
    const playlistFileName = `${audioBaseName}_1.m3u8`;

    const audioArgs = this.encodingArgs.createAudioEncodingArgs({
      inputFile: inputFileUrl || inputFile,
      parsedInput,
      audioParams,
      codec,
      channels: sourceInfo.channels,
      downmix,
      audioIndex: audioTrackIndex,
      outputFileName: encodedAudioFileName
    });

    this.setTranscoderPriority(1);
    await this.encodeMedia(audioArgs, sourceInfo.duration, job.id);
    await this.prepareMediaFile(
      encodedAudioFileName,
      preparedAudioFileName,
      parsedInput,
      `${audioBaseName}_temp`,
      manifestFileName,
      job
    );
    this.setTranscoderPriority(0);

    this.logger.info(
      `Reading audio data: ${preparedAudioFileName}, ${mpdManifestFileName}, ${playlistFileName} and ${manifestFileName}`
    );
    const audioInfo = await FFprobe(`${parsedInput.dir}/${preparedAudioFileName}`, {
      path: `${this.configService.get<string>('FFMPEG_DIR')}/ffprobe`
    });
    const audioTrack = audioInfo.streams.find((s) => s.codec_type === 'audio');
    const audioMIInfo = await mediaInfoHelper.getMediaInfo(
      `${parsedInput.dir}/${preparedAudioFileName}`,
      this.configService.get<string>('MEDIAINFO_DIR')
    );
    const audioMITrack = audioMIInfo.media.track.find((s) => s['@type'] === 'Audio');
    if (!audioTrack || !audioMITrack) throw new Error('Failed to get encoded audio info');
    await manifest.appendAudioPlaylist({
      mpdPath: `${parsedInput.dir}/${mpdManifestFileName}`,
      m3u8PlaylistPath: `${parsedInput.dir}/${playlistFileName}`,
      format: audioMITrack.Format,
      mimeType: 'audio/mp4',
      isDefault: isDefault,
      language: sourceInfo.language || audioMITrack.Language,
      title: sourceInfo.title,
      channels: +audioMITrack.Channels || audioTrack.channels || 2,
      samplingRate: +audioMITrack.SamplingRate || +audioTrack.sample_rate || 0,
      codec: codec,
      uri: `${streamId}/${preparedAudioFileName}`
    });

    const rcloneMoveArgs = this.encodingArgs.createRcloneMoveArgs(
      `${parsedInput.dir}/${preparedAudioFileName}`,
      `${job.data.storage}:${job.data._id}/${streamId}`
    );
    await this.uploadMedia(rcloneMoveArgs, job.id);

    await this.daplexApiService.ensureProducerAppIsOnline(job.data.producerUrl);
    await this.videoResultQueue.add('add-stream-audio', {
      ...job.data,
      jobId: job.id,
      progress: {
        sourceId: job.data._id,
        streamId: streamId,
        fileName: preparedAudioFileName,
        codec: codec,
        channels: +audioMITrack.Channels || audioTrack.channels || 2
      }
    });
  }

  private async encodeByCodec(options: EncodeVideoOptions) {
    const {
      inputFile,
      parsedInput,
      inputFileUrl,
      sourceInfo,
      qualityList,
      encodingSettings,
      advancedSettings = {},
      codec,
      videoParams,
      manifest,
      job
    } = options;
    // Merge default encoding settings with override settings
    if (advancedSettings.overrideSettings) {
      advancedSettings.overrideSettings.forEach((os) => {
        const qualitySettings = encodingSettings.find((s) => s.quality === os.quality);
        if (qualitySettings) Object.assign(qualitySettings, os);
      });
    }
    for (let i = 0; i < qualityList.length; i++) {
      await this.transcoderApiService.checkAndWaitForTranscoderPriority();
      this.logger.info(`Processing video quality: ${qualityList[i]}`);
      const streamId = await createSnowFlakeId();
      const perQualitySettings = encodingSettings.find((s) => s.quality === qualityList[i]);
      const videoBaseName = `${parsedInput.name}_${qualityList[i]}`;
      const encodedVideoFileName =
        codec === VideoCodec.AV1 && sourceInfo.isHDR ? `${videoBaseName}.mkv` : `${videoBaseName}.mp4`;
      const preparedVideoFileName = `${videoBaseName}.mp4`;
      const manifestFileName = `${videoBaseName}.m3u8`;
      const mpdManifestFileName = `${videoBaseName}.mpd`;
      const playlistFileName = `${videoBaseName}_1.m3u8`;
      try {
        if (!this.SplitEncoding) {
          this.setTranscoderPriority(1);
          if (codec === VideoCodec.H264 || codec === VideoCodec.H265 || codec === VideoCodec.AV1) {
            const crfKey = codec === VideoCodec.AV1 ? 'cq' : 'crf';
            const videoArgs = this.encodingArgs.createVideoEncodingArgs({
              inputFile: inputFileUrl || inputFile,
              parsedInput,
              codec,
              quality: qualityList[i],
              videoParams,
              sourceInfo,
              crfKey,
              advancedSettings,
              encodingSetting: perQualitySettings,
              outputFileName: encodedVideoFileName
            });
            await this.encodeMedia(videoArgs, sourceInfo.duration, job.id);
          } else {
            // Pass 1 params
            const videoPass1Args = this.encodingArgs.createTwoPassesVideoEncodingArgs({
              inputFile: inputFileUrl || inputFile,
              parsedInput,
              codec,
              quality: qualityList[i],
              videoParams,
              sourceInfo,
              crfKey: 'cq',
              advancedSettings,
              encodingSetting: perQualitySettings,
              pass: 1,
              outputFileName: encodedVideoFileName
            });
            // Pass 2 params
            const videoPass2Args = this.encodingArgs.createTwoPassesVideoEncodingArgs({
              inputFile: inputFileUrl || inputFile,
              parsedInput,
              codec,
              quality: qualityList[i],
              videoParams,
              sourceInfo,
              crfKey: 'cq',
              advancedSettings,
              encodingSetting: perQualitySettings,
              pass: 2,
              outputFileName: encodedVideoFileName
            });

            await this.encodeMedia(videoPass1Args, sourceInfo.duration, job.id);
            await this.encodeMedia(videoPass2Args, sourceInfo.duration, job.id);
          }
          this.setTranscoderPriority(0);
        } else {
          const segmentDuration = +this.configService.get('SPLIT_SEGMENT_DURATION') || 30;
          await this.splitAndEncodeVideo(
            options,
            qualityList[i],
            perQualitySettings,
            segmentDuration,
            encodedVideoFileName
          );
        }

        this.setTranscoderPriority(1);
        await this.prepareMediaFile(
          encodedVideoFileName,
          preparedVideoFileName,
          parsedInput,
          `${videoBaseName}_temp`,
          manifestFileName,
          job
        );
        this.setTranscoderPriority(0);

        this.logger.info(
          `Reading video data: ${preparedVideoFileName}, ${mpdManifestFileName}, ${playlistFileName} and ${manifestFileName}`
        );
        const videoMIInfo = await mediaInfoHelper.getMediaInfo(
          `${parsedInput.dir}/${preparedVideoFileName}`,
          this.configService.get<string>('MEDIAINFO_DIR')
        );
        const generalMITrack = videoMIInfo.media.track.find((s) => s['@type'] === 'General');
        const videoMITrack = videoMIInfo.media.track.find((s) => s['@type'] === 'Video');
        if (!videoMITrack) throw new Error('Failed to get encoded video info');
        manifest.appendVideoPlaylist({
          mpdPath: `${parsedInput.dir}/${mpdManifestFileName}`,
          m3u8PlaylistPath: `${parsedInput.dir}/${playlistFileName}`,
          width: +videoMITrack.Width || 0,
          height: +videoMITrack.Height || 0,
          format: videoMITrack.Format,
          mimeType: 'video/mp4',
          language: sourceInfo.language || videoMITrack.Language,
          frameRate: +videoMITrack.FrameRate || +generalMITrack?.FrameRate,
          codec: codec,
          uri: `${streamId}/${preparedVideoFileName}`
        });

        const rcloneMoveArgs = this.encodingArgs.createRcloneMoveArgs(
          `${parsedInput.dir}/${preparedVideoFileName}`,
          `${job.data.storage}:${job.data._id}/${streamId}`
        );
        await this.uploadMedia(rcloneMoveArgs, job.id);

        // Save and upload manifest file
        await this.saveManifestFile(manifest, parsedInput.dir, codec, job, sourceInfo);
      } catch (e) {
        const rcloneDir = this.configService.get<string>('RCLONE_DIR');
        const rcloneConfig = this.configService.get<string>('RCLONE_CONFIG_FILE');
        console.error(e);
        this.logger.error(JSON.stringify(e));
        this.logger.info('Removing unprocessed file');
        try {
          await rcloneHelper.deletePath(
            rcloneConfig,
            rcloneDir,
            job.data.storage,
            `${job.data._id}/${streamId}`,
            (args) => {
              this.logger.info('rclone ' + args.join(' '));
            }
          );
        } catch (e) {
          console.error(e);
          this.logger.error(JSON.stringify(e));
        }
        throw e;
      }

      await this.daplexApiService.ensureProducerAppIsOnline(job.data.producerUrl);
      await this.videoResultQueue.add('add-stream-video', {
        ...job.data,
        jobId: job.id,
        progress: {
          sourceId: job.data._id,
          streamId: streamId,
          fileName: preparedVideoFileName,
          codec: codec,
          quality: qualityList[i],
          hdrFormat: sourceInfo.hdrParams?.hdrFormat
        }
      });
    }
  }

  private async splitAndEncodeVideo(
    options: EncodeVideoOptions,
    quality: number,
    perQualitySettings: IEncodingSetting,
    segmentDuration: number = 30,
    outputFileName: string
  ) {
    const {
      inputFile,
      parsedInput,
      inputFileUrl,
      sourceInfo,
      advancedSettings = {},
      codec,
      videoParams,
      job
    } = options;
    const segmentFolder = `${parsedInput.dir}/${SPLIT_SEGMENT_FOLDER}`;
    const concatSegmentFile = `${segmentFolder}/${CONCAT_SEGMENT_FILE}`;
    let totalSegments = Math.ceil(sourceInfo.duration / segmentDuration);
    this.logger.info(`Total segments: ${totalSegments}`);

    //Create segment folder
    await fileHelper.createDir(segmentFolder);
    this.setTranscoderPriority(1);

    for (let i = 0; i < totalSegments; i++) {
      // const handleSegmentError = () => {
      //   this.logger.info('Received error 139 from FFmpeg');
      //   const oldTotalSegments = totalSegments;
      //   // Reduce duration by 10 second, but not below 10
      //   segmentDuration = Math.max(10, segmentDuration - 10);
      //   totalSegments = Math.ceil(sourceInfo.duration / segmentDuration);
      //   if (totalSegments !== oldTotalSegments)
      //     i = Math.ceil(i * totalSegments / oldTotalSegments);
      //   this.logger.info(`New segment duration: ${segmentDuration}, total segments: ${totalSegments}, segment: ${i + 1}`);
      // };
      // Output mkv for dynamic HDR retention when muxing with mp4box, otherwise use mp4 due to H264 compability
      const segmentFileName =
        codec === VideoCodec.AV1 && sourceInfo.isHDR ? `${quality}_${i}.mkv` : `${quality}_${i}.mp4`;
      const segmentFileSubPath = `${SPLIT_SEGMENT_FOLDER}/${segmentFileName}`;
      // Wait until the primary transcoder is not busy
      while (true) {
        await this.transcoderApiService.checkAndWaitForTranscoderPriority();
        const startTime = i * segmentDuration;
        this.logger.info(`Segments ${i + 1}/${totalSegments}`);
        this.CanRetryEncoding = true;
        if (codec === VideoCodec.H264 || codec === VideoCodec.H265 || codec === VideoCodec.AV1) {
          const crfKey = codec === VideoCodec.AV1 ? 'cq' : 'crf';
          const videoArgs = this.encodingArgs.createVideoEncodingArgs({
            inputFile: inputFileUrl || inputFile,
            parsedInput,
            codec,
            quality,
            videoParams,
            sourceInfo,
            crfKey,
            advancedSettings,
            encodingSetting: perQualitySettings,
            splitFrom: startTime.toString(),
            splitDuration: segmentDuration.toString(),
            segmentIndex: i,
            outputFileName: segmentFileSubPath
          });
          try {
            await this.encodeMedia(videoArgs, segmentDuration, job.id);
          } catch (e) {
            if (e === RejectCode.RETRY_ENCODING) {
              this.logger.info('Retrying encoding (user input)');
              continue;
            } else if (e === RejectCode.ENCODING_TIMEOUT) {
              this.logger.info('Retrying encoding (timed out)');
              continue;
            } else if (typeof e === 'object' && e !== null && 'code' in e) {
              // Handle encoding error
              this.logger.info(`Received error ${e.code} from FFmpeg, retrying...`);
              await new Promise((r) => setTimeout(r, 30_000));
              continue;
            }
            throw e;
          }
        } else {
          // Pass 1 params
          const videoPass1Args = this.encodingArgs.createTwoPassesVideoEncodingArgs({
            inputFile: inputFileUrl || inputFile,
            parsedInput,
            codec,
            quality,
            videoParams,
            sourceInfo,
            crfKey: 'cq',
            advancedSettings,
            encodingSetting: perQualitySettings,
            pass: 1,
            splitFrom: startTime.toString(),
            splitDuration: segmentDuration.toString(),
            segmentIndex: i,
            outputFileName: segmentFileSubPath
          });
          // Pass 2 params
          const videoPass2Args = this.encodingArgs.createTwoPassesVideoEncodingArgs({
            inputFile: inputFileUrl || inputFile,
            parsedInput,
            codec,
            quality,
            videoParams,
            sourceInfo,
            crfKey: 'cq',
            advancedSettings,
            encodingSetting: perQualitySettings,
            pass: 2,
            splitFrom: startTime.toString(),
            splitDuration: segmentDuration.toString(),
            segmentIndex: i,
            outputFileName: segmentFileSubPath
          });
          try {
            await this.encodeMedia(videoPass1Args, segmentDuration, job.id);
            await this.encodeMedia(videoPass2Args, segmentDuration, job.id);
          } catch (e) {
            if (e === RejectCode.RETRY_ENCODING) {
              this.logger.info('Retrying encoding (user input)');
              continue;
            } else if (e === RejectCode.ENCODING_TIMEOUT) {
              this.logger.info('Retrying encoding (timed out)');
              continue;
            } else if (typeof e === 'object' && e !== null && 'code' in e) {
              this.logger.info(`Received error ${e.code} from FFmpeg, retrying...`);
              await new Promise((r) => setTimeout(r, 30_000));
              continue;
            }
            throw e;
          }
        }
        this.CanRetryEncoding = false;
        break;
      }

      await fileHelper.appendToFile(concatSegmentFile, `file ${segmentFileName}\n`);
    }

    // Merge back
    const concatSegmentArgs = this.encodingArgs.createConcatSegmentArgs(concatSegmentFile, parsedInput, outputFileName);
    await this.encodeMedia(concatSegmentArgs, sourceInfo.duration, job.id);
    this.setTranscoderPriority(0);

    // Remove segment folder
    await fileHelper.deleteFolder(segmentFolder);
  }

  @Cron('0 0 */5 * *')
  async handleInactiveRefreshToken() {
    // Runs every 5 days
    // Try to refresh all inactive tokens
    this.logger.info('Running scheduled token refresh');
    const rcloneDir = this.configService.get<string>('RCLONE_DIR');
    const rcloneConfig = this.configService.get<string>('RCLONE_CONFIG_FILE');
    const remoteList = await rcloneHelper.findAllRemotes(rcloneConfig, rcloneDir);
    if (!remoteList.length) return;
    await rcloneHelper.refreshRemoteTokens(rcloneConfig, rcloneDir, remoteList, (args) => {
      this.logger.info('rclone ' + args.join(' '));
    });
  }

  private async prepareMediaFile(
    inputFileName: string,
    outputFileName: string,
    parsedInput: path.ParsedPath,
    tempFileName: string,
    playlistName: string,
    job: Job<IVideoData>
  ) {
    this.logger.info(`Preparing media file: ${inputFileName}`);
    // Trim saved file name
    const trimmedFileName = job.data.linkedStorage
      ? stringHelper.trimSlugFilename(job.data.filename)
      : job.data.filename;
    const inputFilePath = `${parsedInput.dir}/${inputFileName}`;
    const outputFilePath = `${parsedInput.dir}/${outputFileName}`;
    const inputSourceFile = `${parsedInput.dir}/${trimmedFileName}`;
    const hasFreeSpace = await diskSpaceUtil.hasFreeSpaceToCopyFile(inputFilePath, parsedInput.dir);
    if (!hasFreeSpace) {
      this.logger.warning(`Not enough disk space to duplicate file, deleting: ${trimmedFileName} temporary`);
      await fileHelper.deleteFile(inputSourceFile);
    }
    const mp4boxPackArgs = this.encodingArgs.createMP4BoxPackArgs(
      inputFilePath,
      parsedInput,
      tempFileName,
      playlistName
    );
    await this.packageMedia(mp4boxPackArgs, job.id);
    await fileHelper.deleteFile(inputFilePath);
    const tempFilePath = `${parsedInput.dir}/${tempFileName}.mp4`;
    await fileHelper.renameFile(tempFilePath, outputFilePath);
    if (!hasFreeSpace) {
      this.logger.info(`Redownloading: ${job.data.filename}`);
      const rcloneDir = this.configService.get<string>('RCLONE_DIR');
      const rcloneConfigFile = this.configService.get<string>('RCLONE_CONFIG_FILE');
      const downloadStorage = job.data.linkedStorage || job.data.storage;
      await rcloneHelper.downloadFile(
        rcloneConfigFile,
        rcloneDir,
        downloadStorage,
        job.data.path,
        job.data.filename,
        parsedInput.dir,
        !!job.data.linkedStorage,
        (args) => {
          this.logger.info('rclone ' + args.join(' '));
        }
      );
      if (job.data.linkedStorage) {
        // Set trimmed file name
        await fileHelper.renameFile(`${parsedInput.dir}/${job.data.filename}`, inputSourceFile);
      }
    }
  }

  private async saveManifestFile(
    manifest: StreamManifest,
    transcodeDir: string,
    codec: number,
    job: Job<IVideoData>,
    sourceInfo?: VideoSourceInfo
  ) {
    const manifestFileName = `manifest_${codec}.json`;
    const manifestFilePath = `${transcodeDir}/${manifestFileName}`;
    const streamId = await createSnowFlakeId();
    this.logger.info(`Generating manifest file: ${manifestFileName}`);
    await manifest.saveFile(manifestFilePath);
    const rcloneMoveManifestArgs = this.encodingArgs.createRcloneMoveArgs(
      manifestFilePath,
      `${job.data.storage}:${job.data._id}/${streamId}`
    );
    await this.uploadMedia(rcloneMoveManifestArgs, job.id);
    await this.videoResultQueue.add('add-stream-manifest', {
      ...job.data,
      jobId: job.id,
      progress: {
        sourceId: job.data._id,
        streamId: streamId,
        fileName: manifestFileName,
        codec: codec,
        hdrFormat: sourceInfo?.hdrParams?.hdrFormat
      }
    });
  }

  private encodeMedia(args: string[], videoDuration: number, jobId: string | number) {
    return this.spawner.encodeMedia(args, videoDuration, jobId);
  }

  private packageMedia(args: string[], jobId: string | number) {
    return this.spawner.packageMedia(args, jobId);
  }

  private uploadMedia(args: string[], jobId: string | number) {
    return this.spawner.uploadMedia(args, jobId);
  }

  private findUploadedFiles(remote: string, parentFolder: string, jobId: string | number, exclude?: string) {
    return this.spawner.findUploadedFiles(remote, parentFolder, jobId, exclude);
  }

  private ensureRcloneConfigExist(configFile: string, storage: string, job: Job<IVideoData>) {
    return this.rclone.ensureRcloneConfigExist(configFile, storage, job, (j) =>
      this.generateStatusError(StatusCode.STORAGE_NOT_FOUND, j)
    );
  }

  private getLinkedSourceUrl(job: Job<IVideoData>) {
    return this.rclone.getLinkedSourceUrl(job, (j) => this.generateStatusError(StatusCode.STORAGE_NOT_FOUND, j));
  }

  private async validateSourceQuality(options: ValidateSourceQualityOptions): Promise<number[] | null> {
    const {
      parsedInput,
      quality,
      qualityList,
      forcedQualityList,
      fallbackQualityList,
      codec,
      retryFromInterruption,
      job
    } = options;
    const allQualityList = this.qualityResolver.calculateQuality(
      quality,
      qualityList,
      forcedQualityList,
      fallbackQualityList
    );
    this.logger.info(`All quality: ${allQualityList.length ? allQualityList.join(', ') : 'None'}`);
    // if (!allQualityList.length) {
    //   const statusError = await this.generateStatusError(StatusCode.LOW_QUALITY_VIDEO, job, { discard: true });
    //   throw new UnrecoverableError(statusError.errorCode);
    // }
    let availableQualityList: number[];
    if (!retryFromInterruption) {
      // Check already encoded files
      this.logger.info('Checking already encoded files');
      let alreadyEncodedFiles: string[] = [];
      const existingManifestData = await this.qualityResolver.findExistingManifest(
        job.data.storage,
        job.data._id,
        codec
      );
      if (existingManifestData?.videoTracks) alreadyEncodedFiles = existingManifestData.videoTracks.map((t) => t.uri);
      availableQualityList = await this.qualityResolver.findAvailableQuality(
        alreadyEncodedFiles,
        allQualityList,
        parsedInput,
        codec,
        job.data.replaceStreams,
        job
      );
      this.logger.info(`Available quality: ${availableQualityList.length ? availableQualityList.join(', ') : 'None'}`);
      if (!availableQualityList.length && !job.data.advancedOptions?.audioOnly) {
        this.logger.info('Everything is already encoded, no need to continue');
        await this.daplexApiService.ensureProducerAppIsOnline(job.data.producerUrl);
        await this.videoResultQueue.add('cancelled-encoding', { ...job.data, jobId: job.id, keepStreams: true });
        return null;
      }
    } else {
      availableQualityList = [...allQualityList];
    }
    // Ensure the folder is empty if we need to encode all the qualities
    if (allQualityList.length === availableQualityList.length && retryFromInterruption) {
      const rcloneConfigFile = this.configService.get<string>('RCLONE_CONFIG_FILE');
      const rcloneDir = this.configService.get<string>('RCLONE_DIR');
      this.logger.info('Cleanning source folder');
      await rcloneHelper.emptyPath(
        rcloneConfigFile,
        rcloneDir,
        job.data.storage,
        `${job.data._id}/*`,
        (args) => {
          this.logger.info('rclone ' + args.join(' '));
        },
        {
          include: '*/**'
        }
      );
    }
    return availableQualityList;
  }

  private async generateStatusError(
    errorCode: string,
    job: Job<IVideoData>,
    options: { discard: boolean } = { discard: false }
  ) {
    const status = { errorCode, jobId: job.id, ...job.data };
    const statusJson = JSON.stringify(status);
    this.logger.error(`Error: ${errorCode} - ${statusJson}`);
    await this.daplexApiService.ensureProducerAppIsOnline(job.data.producerUrl);
    if (options.discard) job.discard();
    if (options.discard || job.attemptsMade >= job.opts.attempts)
      await this.videoResultQueue.add('failed-encoding', status);
    else if (job.attemptsMade < job.opts.attempts) await this.videoResultQueue.add('retry-encoding', status);
    return status;
  }

  private generateStatus(job: Job<IVideoData>) {
    return { jobId: job.id, ...job.data };
  }
}
