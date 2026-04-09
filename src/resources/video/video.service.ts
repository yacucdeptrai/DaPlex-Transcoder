import { Inject, Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron } from '@nestjs/schedule';
import { InjectQueue } from '@nestjs/bullmq';
import { Job, Queue, UnrecoverableError } from 'bullmq';
import mongoose from 'mongoose';
import { stdout } from 'process';
import child_process from 'child_process';
import path from 'path';
import FFprobe from 'ffprobe-client';
import { WINSTON_MODULE_PROVIDER } from 'nest-winston';
import { Logger } from 'winston';

import { externalStorageModel } from '../../models/external-storage.model';
import { mediaStorageModel } from '../../models/media-storage.model';
import { settingModel } from '../../models/setting.model';
import { mediaModel } from '../../models/media.model';
import { IVideoData, IJobData, IStorage, IEncodingSetting, MediaQueueResult, EncodeAudioOptions, EncodeVideoOptions, VideoSourceInfo, CreateAudioEncodingArgsOptions, CreateVideoEncodingArgsOptions, EncodeAudioByTrackOptions, AdvancedVideoSettings, ResolveVideoFiltersOptions, ValidateSourceQualityOptions } from './interfaces';
import { AudioCodec, StatusCode, VideoCodec, RejectCode, TaskQueue } from '../../enums';
import { ENCODING_QUALITY, AUDIO_PARAMS, AUDIO_SURROUND_PARAMS, VIDEO_H264_PARAMS, VIDEO_H265_PARAMS, VIDEO_VP9_PARAMS, VIDEO_AV1_PARAMS, AUDIO_SPEED_PARAMS, AUDIO_SURROUND_OPUS_PARAMS, NEXT_GEN_ENCODING_QUALITY, SPLIT_SEGMENT_FOLDER, CONCAT_SEGMENT_FILE } from '../../config';
import { HlsManifest, RcloneFile } from '../../common/interfaces';
import { DaplexApiService } from '../../common/modules/daplex-api';
import { TranscoderApiService } from '../../common/modules/transcoder-api';
import {
  createSnowFlakeId, diskSpaceUtil, ffmpegHelper, fileHelper, generateSprites, hdrMetadataHelper, mediaInfoHelper,
  MediaInfoResult, StringCrypto, stringHelper, StreamManifest, rcloneHelper, videoSourceHelper, isEqualShallow
} from '../../utils';
import { Progress } from '../../common/entities';

type JobNameType = 'update-source' | 'add-stream-video' | 'add-stream-audio' | 'add-stream-manifest' | 'finished-encoding' |
  'cancelled-encoding' | 'retry-encoding' | 'failed-encoding';

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

  constructor(@Inject(WINSTON_MODULE_PROVIDER) private readonly logger: Logger,
    @InjectQueue(TaskQueue.VIDEO_TRANSCODE_RESULT) private videoResultQueue: Queue<MediaQueueResult, any, JobNameType>,
    private configService: ConfigService, private daplexApiService: DaplexApiService,
    private transcoderApiService: TranscoderApiService) {
    const audioParams = this.configService.get<string>('AUDIO_PARAMS');
    this.AudioParams = audioParams ? audioParams.split(' ') : AUDIO_PARAMS;
    const audioSpeedParams = this.configService.get<string>('AUDIO_SPEED_PARAMS');
    this.AudioSpeedParams = audioSpeedParams ? audioSpeedParams.split(' ') : AUDIO_SPEED_PARAMS;
    const audioSurroundParams = this.configService.get<string>('AUDIO_SURROUND_PARAMS');
    this.AudioSurroundParams = audioSurroundParams ? audioSurroundParams.split(' ') : AUDIO_SURROUND_PARAMS;
    const audioSurroundOpusParams = this.configService.get<string>('AUDIO_SURROUND_OPUS_PARAMS');
    this.AudioSurroundOpusParams = audioSurroundOpusParams ? audioSurroundOpusParams.split(' ') : AUDIO_SURROUND_OPUS_PARAMS;
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
    this.thumbnailFolder = 'thumbnails';
  }

  async transcode(job: Job<IVideoData>, codec: number = 1) {
    const cancelIndex = this.CanceledJobIds.findIndex(j => +j === +job.id);
    if (cancelIndex > -1) {
      this.CanceledJobIds = this.CanceledJobIds.filter(id => +id > +job.id);
      this.logger.info(`Received cancel signal from job id: ${job.id}`);
      return {};
    }

    // Connect to MongoDB
    await mongoose.connect(this.configService.get<string>('DATABASE_URL'), { family: 4, useBigInt64: true });
    const appSettings = await settingModel.findOne({}).lean().exec();
    const mediaInfo = await mediaModel.findOne({ _id: BigInt(job.data.media) }, { _id: 1, originalLang: 1 }).lean().exec();
    const streamStorage = await externalStorageModel.findOne({ _id: BigInt(job.data.storage) }, { _id: 1, publicUrl: 1 }).lean().exec();

    const audioParams = appSettings.audioParams ? appSettings.audioParams.split(' ') : this.AudioParams;
    const audioSpeedParams = appSettings.audioSpeedParams ? appSettings.audioSpeedParams.split(' ') : this.AudioSpeedParams;
    const audioSurroundParams = appSettings.audioSurroundParams ? appSettings.audioSurroundParams.split(' ') : this.AudioSurroundParams;
    const audioSurroundOpusParams = appSettings.audioSurroundOpusParams ? appSettings.audioSurroundOpusParams.split(' ') : this.AudioSurroundOpusParams;
    const videoH264Params = appSettings.videoH264Params ? appSettings.videoH264Params.split(' ') : this.VideoH264Params;
    const videoH265Params = appSettings.videoH265Params ? appSettings.videoH265Params.split(' ') : this.VideoH265Params;
    const videoVP9Params = appSettings.videoVP9Params ? appSettings.videoVP9Params.split(' ') : this.VideoVP9Params;
    const videoAV1Params = appSettings.videoAV1Params ? appSettings.videoAV1Params.split(' ') : this.VideoAV1Params;
    const qualityList = VideoCodec.H264 === codec ?
      (Array.isArray(appSettings.videoQualityList) && appSettings.videoQualityList.length ? appSettings.videoQualityList : ENCODING_QUALITY) :
      (Array.isArray(appSettings.videoNextGenQualityList) && appSettings.videoNextGenQualityList.length ? appSettings.videoNextGenQualityList : NEXT_GEN_ENCODING_QUALITY);
    const encodingSettings = appSettings.videoEncodingSettings || [];

    const rcloneDir = this.configService.get<string>('RCLONE_DIR');
    const rcloneConfigFile = this.configService.get<string>('RCLONE_CONFIG_FILE');
    const transcodeDir = `${this.configService.get<string>('TRANSCODE_DIR')}/${job.id}`;
    const ffmpegDir = this.configService.get<string>('FFMPEG_DIR');
    const mediainfoDir = this.configService.get<string>('MEDIAINFO_DIR');
    const trimmedFileName = job.data.linkedStorage ? stringHelper.trimSlugFilename(job.data.filename) : job.data.filename; // Trim saved file name
    const inputFile = `${transcodeDir}/${trimmedFileName}`;
    const parsedInput = path.parse(inputFile);

    await this.ensureRcloneConfigExist(rcloneConfigFile, job.data.storage, job);
    if (job.data.linkedStorage)
      await this.ensureRcloneConfigExist(rcloneConfigFile, job.data.linkedStorage, job);

    let linkedInputUrl = this.UseURLInput ? await this.getLinkedSourceUrl(job) : null;

    // Retry if the transcoder was interrupted before
    const retryFromInterruption = await fileHelper.fileExists(transcodeDir);
    if (retryFromInterruption) {
      this.logger.notice('Transcode directory detected, maybe the transcoder was not exited properly before, cleaning up...');
      const status = { jobId: job.id, ...job.data };
      await this.videoResultQueue.add('retry-encoding', status);
      await fileHelper.deleteFolder(transcodeDir);
    }

    let availableQualityList: number[] | null = null;
    const forcedQualityList = job.data.advancedOptions?.forceVideoQuality || [];
    // Find and validate source quality if the quality is available on db
    {
      const sourceInfo = await mediaStorageModel.findOne({ _id: BigInt(job.data._id) }, { _id: 1, name: 1, quality: 1 }).lean().exec();
      if (sourceInfo?.quality) {
        try {
          availableQualityList = await this.validateSourceQuality({
            parsedInput, quality: sourceInfo.quality, qualityList, forcedQualityList, fallbackQualityList: [Math.min(...qualityList)],
            codec, retryFromInterruption, job
          });
          if (availableQualityList === null)
            return {}; // There's nothing to encode
        } finally {
          if (availableQualityList === null)
            await fileHelper.deleteFolder(transcodeDir);
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
          if (downloadedFileStats)
            await fileHelper.deleteFile(inputFile); // Delete file if exist
          const downloadStorage = job.data.linkedStorage || job.data.storage;
          await rcloneHelper.downloadFile(rcloneConfigFile, rcloneDir, downloadStorage, job.data.path, job.data.filename, transcodeDir,
            !!job.data.linkedStorage, (args => {
              this.logger.info('rclone ' + args.join(' '));
            }));
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
        this.logger.info(`Processing input file: ${inputFile}`);
        videoInfo = await FFprobe(inputFile, { path: `${ffmpegDir}/ffprobe` });
        videoMIInfo = await mediaInfoHelper.getMediaInfo(inputFile, mediainfoDir);
      } else {
        this.logger.info(`Processing input file: ${linkedInputUrl}`);
        videoInfo = await FFprobe(linkedInputUrl, { path: `${ffmpegDir}/ffprobe` });
        videoMIInfo = await mediaInfoHelper.getMediaInfo(linkedInputUrl, mediainfoDir);
      }
    } catch (e) {
      console.error(e);
      this.logger.error(e);
      await fileHelper.deleteFolder(transcodeDir);
      const statusError = await this.generateStatusError(StatusCode.PROBE_FAILED, job, { discard: true });
      throw new UnrecoverableError(statusError.errorCode);
    }

    const videoTrack = videoInfo.streams.find(s => s.codec_type === 'video');
    const videoMITrack = videoMIInfo.media.track.find(s => s['@type'] === 'Video');
    if (!videoTrack || !videoMITrack) {
      this.logger.error('Video track not found');
      await fileHelper.deleteFolder(transcodeDir);
      const statusError = await this.generateStatusError(StatusCode.NO_VIDEO_TRACK, job, { discard: true });
      throw new UnrecoverableError(statusError.errorCode);
    }

    const audioTracks = videoInfo.streams.filter(s => s.codec_type === 'audio');
    if (!audioTracks.length) {
      this.logger.error('Audio track not found');
      await fileHelper.deleteFolder(transcodeDir);
      const statusError = await this.generateStatusError(StatusCode.NO_AUDIO_TRACK, job, { discard: true });
      throw new UnrecoverableError(statusError.errorCode);
    }

    const runtime = videoInfo.format.duration ? Math.trunc(+videoInfo.format.duration) : 0;
    const videoDuration = videoTrack.duration ? Math.trunc(+videoTrack.duration) : runtime;
    const videoFps = mediaInfoHelper.getVideoFrameRate(videoTrack.avg_frame_rate, videoTrack.r_frame_rate, videoMITrack.FrameRate);
    const videoBitrate = videoTrack.bit_rate ? Math.round(+videoTrack.bit_rate / 1000) :
      videoMITrack.BitRate ? Math.round(+videoMITrack.BitRate / 1000) : 0; // Bitrate in Kbps
    const videoCodec = videoTrack.codec_name || '';
    const videoSourceH264Params = (videoCodec === 'h264' && videoMITrack.Encoded_Library_Settings) ?
      videoMITrack.Encoded_Library_Settings : '';

    // Validate source file by reading the local file
    if (!availableQualityList) {
      try {
        availableQualityList = await this.validateSourceQuality({
          parsedInput, quality: videoTrack.height, qualityList, forcedQualityList, fallbackQualityList: [Math.min(...qualityList)],
          codec, retryFromInterruption, job
        });
        if (availableQualityList === null)
          return {}; // There's nothing to encode
      } finally {
        if (availableQualityList === null)
          await fileHelper.deleteFolder(transcodeDir);
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
      const existingManifestData = await this.findExistingManifest(job.data.storage, job.data._id, codec);
      if (existingManifestData !== null) {
        manifest.load(existingManifestData);
        job.data.advancedOptions?.audioOnly && manifest.clearTracks('audio');
        job.data.advancedOptions?.videoOnly && manifest.clearTracks('video');
      }
    }

    // Skip audio encoding for other codecs
    // Only encode if there's no audio track inside the manifest data
    if (codec === VideoCodec.H264 && !job.data.advancedOptions?.videoOnly /*&& manifest.manifest.audioTracks.length === 0*/) {
      this.logger.info('Processing audio');
      const defaultAudioTrack = audioTracks.find(a => a.disposition.default) || audioTracks[0];
      const allowedAudioTracks = new Set(job.data.advancedOptions?.selectAudioTracks || []);
      if (allowedAudioTracks.size === 0)
        allowedAudioTracks.add(defaultAudioTrack.index);

      const audioNormalTrack = audioTracks.find(a => a.channels <= 2 && allowedAudioTracks.has(a.index));
      const audioSurroundTrack = audioTracks.find(a => a.channels > 2 && allowedAudioTracks.has(a.index));
      const audioPrimaryTracks = [audioNormalTrack, audioSurroundTrack].filter(a => a != null);
      const allowedExtraAudioTracks = new Set(job.data.advancedOptions?.extraAudioTracks || []);
      const audioExtraTracks = audioTracks.filter(a => !audioPrimaryTracks.includes(a) && allowedExtraAudioTracks.has(a.index));

      const firstAudioTrack = audioNormalTrack || audioSurroundTrack || defaultAudioTrack;
      const secondAudioTrack = audioSurroundTrack;

      try {
        // Audio language for primary track
        const audioOriginalLang = mediaInfo.originalLang;
        // Encode surround audio track
        if (secondAudioTrack != null) {
          this.logger.info(`Audio track index ${secondAudioTrack.index} (surround)`);
          await this.encodeAudioByTrack({
            inputFile, parsedInput, type: 'surround', audioTrack: secondAudioTrack,
            audioAACParams: audioSurroundParams, audioOpusParams: audioSurroundOpusParams, isDefault: true, downmix: false,
            language: audioOriginalLang, manifest, job
          });
        }
        // Encode stereo or mono audio track
        this.logger.info(`Audio track index ${firstAudioTrack.index} (normal)`);
        await this.encodeAudioByTrack({
          inputFile, parsedInput, type: 'normal', audioTrack: firstAudioTrack,
          audioAACParams: audioParams, audioOpusParams: audioSpeedParams, isDefault: !secondAudioTrack,
          downmix: firstAudioTrack.channels > 2, language: audioOriginalLang, manifest, job
        });
        // Encode any others audio tracks
        for (let i = 0; i < audioExtraTracks.length; i++) {
          const extraAudioTrack = audioExtraTracks[i];
          const extraTrackLang = extraAudioTrack.tags?.language || 'N/A';
          const extraTrackType = extraAudioTrack.channels > 2 ? 'surround' : 'normal';
          const extraAACParams = extraAudioTrack.channels > 2 ? audioSurroundParams : audioParams;
          const extraOpusParams = extraAudioTrack.channels > 2 ? audioSurroundOpusParams : audioSpeedParams;
          this.logger.info(`Audio track index ${extraAudioTrack.index} (others, channels: ${extraAudioTrack.channels}, language: ${extraTrackLang})`);
          await this.encodeAudioByTrack({
            inputFile, parsedInput, type: extraTrackType, audioTrack: extraAudioTrack,
            audioAACParams: extraAACParams, audioOpusParams: extraOpusParams, isDefault: false, downmix: false, manifest, job
          });
        }
        // Save and update manifest file when encoding audio only
        if (job.data.advancedOptions?.audioOnly) {
          await this.saveManifestFile(manifest, parsedInput.dir, codec, job);
        }
        // Delete the downloaded source file after the audio is processed, continue using URL
        if (this.UseURLInput)
          await fileHelper.deleteFile(inputFile);
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
            logFn: (message) => { this.logger.info(message) }
          });
          const moveRemuxFileArgs = this.createRcloneMoveArgs(remuxFilePath, remuxUrlFolder);
          await this.uploadMedia(moveRemuxFileArgs, job.id);
          linkedInputUrl = streamStorage.publicUrl.replace(':service_path', 's3').replace(':path', remuxUrlPath);
        }
        // Video info
        const isHDRVideo = mediaInfoHelper.isHDRVideo(videoTrack.color_space, videoTrack.color_transfer, videoTrack.color_primaries);
        const hdrParams = isHDRVideo && codec !== VideoCodec.H264 ?
          await hdrMetadataHelper.getHdrMetadata(linkedInputUrl || inputFile, 0, ffmpegDir, this.logger) : null;
        const sourceInfo: VideoSourceInfo = {
          duration: videoDuration, fps: videoFps, bitrate: videoBitrate, codec: videoCodec, sourceH264Params: videoSourceH264Params,
          width: srcWidth, height: srcHeight, language: mediaInfo.originalLang, isHDR: isHDRVideo, hdrParams: hdrParams
        };
        if (codec === VideoCodec.H264) {
          this.logger.info('Video codec: H264');
          await this.encodeByCodec({
            inputFile, parsedInput, inputFileUrl: linkedInputUrl, sourceInfo, qualityList: availableQualityList, encodingSettings,
            advancedSettings: job.data.advancedOptions, codec: VideoCodec.H264, videoParams: videoH264Params, manifest, job
          });
        } else if (codec === VideoCodec.H265) {
          this.logger.info('Video codec: H265');
          await this.encodeByCodec({
            inputFile, parsedInput, inputFileUrl: linkedInputUrl, sourceInfo, qualityList: availableQualityList, encodingSettings,
            advancedSettings: job.data.advancedOptions, codec: VideoCodec.H265, videoParams: videoH265Params, manifest, job
          });
        } else if (codec === VideoCodec.VP9) {
          this.logger.info('Video codec: VP9');
          await this.encodeByCodec({
            inputFile, parsedInput, inputFileUrl: linkedInputUrl, sourceInfo, qualityList: availableQualityList, encodingSettings,
            advancedSettings: job.data.advancedOptions, codec: VideoCodec.VP9, videoParams: videoVP9Params, manifest, job
          });
        } else if (codec === VideoCodec.AV1) {
          this.logger.info('Video codec: AV1');
          await this.encodeByCodec({
            inputFile, parsedInput, inputFileUrl: linkedInputUrl, sourceInfo, qualityList: availableQualityList, encodingSettings,
            advancedSettings: job.data.advancedOptions, codec: VideoCodec.AV1, videoParams: videoAV1Params, manifest, job
          });
        }

        if (codec === VideoCodec.H264) {
          // Generate preview thumbnail
          this.logger.info(`Generating preview thumbnail: ${inputFile}`);
          this.setTranscoderPriority(1);
          await generateSprites({
            source: linkedInputUrl || inputFile,
            output: `${parsedInput.dir}/${this.thumbnailFolder}`,
            duration: videoDuration,
            isHDR: isHDRVideo,
            ffmpegDir,
            useURLInput: this.UseURLInput,
            jobId: job.id,
            canceledJobIds: this.CanceledJobIds,
            logger: this.logger
          }, [
            { tw: 160, th: 160, pageCols: 10, pageRows: 10, prefix: 'M', format: 'jpeg' },
            { tw: 320, th: 320, pageCols: 5, pageRows: 5, prefix: 'L', format: 'jpeg' }
          ]);
          this.setTranscoderPriority(0);
          const syncThumbnails = !!job.data.update;
          const rcloneMoveThumbArgs = this.createRcloneMoveThumbArgs(transcodeDir, job.data.storage, job.data._id, syncThumbnails);
          await this.uploadMedia(rcloneMoveThumbArgs, job.id);
        }
      }

      if (job.data.replaceStreams?.length) {
        this.logger.info('Removing old streams');
        for (let i = 0; i < job.data.replaceStreams.length; i++) {
          await rcloneHelper.deletePath(rcloneConfigFile, rcloneDir,
            job.data.storage, `${job.data._id}/${job.data.replaceStreams[i]}`, (args => {
              this.logger.info('rclone ' + args.join(' '));
            }))
        }
      }
      // Check uploaded files
      this.logger.info('Checking uploaded files');
      const checkFilesExclusion = `${this.thumbnailFolder}/**`;
      let uploadedFiles = await this.findUploadedFiles(job.data.storage, job.data._id, job.id, checkFilesExclusion);
      let listAttempt = 1;
      // 1 source file (0 for linked source), 3 audio files, and video files
      const expectedVideoFiles = !job.data.advancedOptions?.audioOnly ? availableQualityList.length : 0;
      const expectedAudioFiles = !job.data.advancedOptions?.videoOnly ? 3 : 0;
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
        await rcloneHelper.deleteFile(rcloneConfigFile, rcloneDir, job.data.storage, `${job.data._id}/${remuxFileName}`, (args => {
          this.logger.info('rclone ' + args.join(' '));
        }));
      }
      this.setTranscoderPriority(0);
      this.logger.info('Completed');
    }
    await this.daplexApiService.ensureProducerAppIsOnline(job.data.producerUrl);
    await this.videoResultQueue.add('finished-encoding', this.generateStatus(job));
    return {};
  }

  addToCanceled(jobData: IJobData) {
    if (jobData.id)
      this.CanceledJobIds.push(jobData.id);
    else if (jobData.ids)
      this.CanceledJobIds.push(...jobData.ids);
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
    const { inputFile, parsedInput, inputFileUrl, type, audioTrack, audioAACParams, audioOpusParams, isDefault, downmix, language, manifest, job } = options;
    const aacType = type === 'normal' ? AudioCodec.AAC : AudioCodec.AAC_SURROUND;
    const opusType = type === 'normal' ? AudioCodec.OPUS : AudioCodec.OPUS_SURROUND;
    this.logger.info('Audio codec: AAC');
    const audioDuration = audioTrack.duration ? Math.trunc(+audioTrack.duration) : 0;
    const audioChannels = audioTrack.channels || (type === 'normal' ? 2 : 0);
    const audioTitle = audioTrack.tags?.title || null;
    await this.encodeAudio({
      inputFile, parsedInput, inputFileUrl, sourceInfo: { duration: audioDuration, channels: audioChannels, language, title: audioTitle },
      audioTrackIndex: audioTrack.index, codec: aacType, isDefault, downmix, audioParams: audioAACParams,
      manifest, job
    });
    // Only encode opus surround if the source audio has 5 (4.1), 6 (5.1), 7 (6.1) or 8 (7.1) channels
    if (type === 'normal' || [5, 6, 7, 8].includes(audioChannels)) {
      this.logger.info('Audio codec: OPUS');
      await this.encodeAudio({
        inputFile, parsedInput, inputFileUrl, sourceInfo: { duration: audioDuration, channels: audioChannels, language, title: audioTitle },
        audioTrackIndex: audioTrack.index, codec: opusType, isDefault: false, downmix, audioParams: audioOpusParams,
        manifest, job
      });
    }
  }

  private async encodeAudio(options: EncodeAudioOptions) {
    const { inputFile, parsedInput, inputFileUrl, sourceInfo, audioTrackIndex, codec, isDefault, downmix, audioParams, manifest, job } = options;
    const streamId = await createSnowFlakeId();

    const audioBaseName = `${parsedInput.name}_audio_${audioTrackIndex}`;
    const encodedAudioFileName = `${audioBaseName}.mp4`;
    const preparedAudioFileName = `${audioBaseName}.mp4`;
    const manifestFileName = `${audioBaseName}.m3u8`;
    const mpdManifestFileName = `${audioBaseName}.mpd`;
    const playlistFileName = `${audioBaseName}_1.m3u8`;

    const audioArgs = this.createAudioEncodingArgs({
      inputFile: inputFileUrl || inputFile, parsedInput, audioParams, codec, channels: sourceInfo.channels,
      downmix, audioIndex: audioTrackIndex, outputFileName: encodedAudioFileName
    });

    this.setTranscoderPriority(1);
    await this.encodeMedia(audioArgs, sourceInfo.duration, job.id);
    await this.prepareMediaFile(encodedAudioFileName, preparedAudioFileName, parsedInput, `${audioBaseName}_temp`, manifestFileName, job);
    this.setTranscoderPriority(0);

    this.logger.info(`Reading audio data: ${preparedAudioFileName}, ${mpdManifestFileName}, ${playlistFileName} and ${manifestFileName}`);
    const audioInfo = await FFprobe(`${parsedInput.dir}/${preparedAudioFileName}`, { path: `${this.configService.get<string>('FFMPEG_DIR')}/ffprobe` });
    const audioTrack = audioInfo.streams.find(s => s.codec_type === 'audio');
    const audioMIInfo = await mediaInfoHelper.getMediaInfo(`${parsedInput.dir}/${preparedAudioFileName}`,
      this.configService.get<string>('MEDIAINFO_DIR'));
    const audioMITrack = audioMIInfo.media.track.find(s => s['@type'] === 'Audio');
    if (!audioTrack || !audioMITrack)
      throw new Error('Failed to get encoded audio info');
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

    const rcloneMoveArgs = this.createRcloneMoveArgs(`${parsedInput.dir}/${preparedAudioFileName}`,
      `${job.data.storage}:${job.data._id}/${streamId}`);
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
        channels: +audioMITrack.Channels || audioTrack.channels || 2,
      }
    });
  }

  private async encodeByCodec(options: EncodeVideoOptions) {
    const {
      inputFile, parsedInput, inputFileUrl, sourceInfo, qualityList, encodingSettings, advancedSettings = {}, codec, videoParams,
      manifest, job
    } = options;
    // Merge default encoding settings with override settings
    if (advancedSettings.overrideSettings) {
      advancedSettings.overrideSettings.forEach(os => {
        const qualitySettings = encodingSettings.find(s => s.quality === os.quality);
        if (qualitySettings)
          Object.assign(qualitySettings, os);
      });
    }
    for (let i = 0; i < qualityList.length; i++) {
      await this.transcoderApiService.checkAndWaitForTranscoderPriority();
      this.logger.info(`Processing video quality: ${qualityList[i]}`);
      const streamId = await createSnowFlakeId();
      const perQualitySettings = encodingSettings.find(s => s.quality === qualityList[i]);
      const videoBaseName = `${parsedInput.name}_${qualityList[i]}`;
      const encodedVideoFileName = codec === VideoCodec.AV1 && sourceInfo.isHDR ? `${videoBaseName}.mkv` : `${videoBaseName}.mp4`;
      const preparedVideoFileName = `${videoBaseName}.mp4`;
      const manifestFileName = `${videoBaseName}.m3u8`;
      const mpdManifestFileName = `${videoBaseName}.mpd`;
      const playlistFileName = `${videoBaseName}_1.m3u8`;
      try {
        if (!this.SplitEncoding) {
          this.setTranscoderPriority(1);
          if (codec === VideoCodec.H264 || codec === VideoCodec.H265 || codec === VideoCodec.AV1) {
            const crfKey = codec === VideoCodec.AV1 ? 'cq' : 'crf';
            const videoArgs = this.createVideoEncodingArgs({
              inputFile: inputFileUrl || inputFile, parsedInput, codec, quality: qualityList[i], videoParams,
              sourceInfo, crfKey, advancedSettings, encodingSetting: perQualitySettings, outputFileName: encodedVideoFileName
            });
            await this.encodeMedia(videoArgs, sourceInfo.duration, job.id);
          } else {
            // Pass 1 params
            const videoPass1Args = this.createTwoPassesVideoEncodingArgs({
              inputFile: inputFileUrl || inputFile, parsedInput, codec, quality: qualityList[i], videoParams,
              sourceInfo, crfKey: 'cq', advancedSettings, encodingSetting: perQualitySettings, pass: 1,
              outputFileName: encodedVideoFileName
            });
            // Pass 2 params
            const videoPass2Args = this.createTwoPassesVideoEncodingArgs({
              inputFile: inputFileUrl || inputFile, parsedInput, codec, quality: qualityList[i], videoParams,
              sourceInfo, crfKey: 'cq', advancedSettings, encodingSetting: perQualitySettings, pass: 2,
              outputFileName: encodedVideoFileName
            });

            await this.encodeMedia(videoPass1Args, sourceInfo.duration, job.id);
            await this.encodeMedia(videoPass2Args, sourceInfo.duration, job.id);
          }
          this.setTranscoderPriority(0);
        } else {
          const segmentDuration = +this.configService.get('SPLIT_SEGMENT_DURATION') || 30;
          await this.splitAndEncodeVideo(options, qualityList[i], perQualitySettings, segmentDuration, encodedVideoFileName);
        }

        this.setTranscoderPriority(1);
        await this.prepareMediaFile(encodedVideoFileName, preparedVideoFileName, parsedInput, `${videoBaseName}_temp`, manifestFileName, job);
        this.setTranscoderPriority(0);

        this.logger.info(`Reading video data: ${preparedVideoFileName}, ${mpdManifestFileName}, ${playlistFileName} and ${manifestFileName}`);
        const videoMIInfo = await mediaInfoHelper.getMediaInfo(`${parsedInput.dir}/${preparedVideoFileName}`,
          this.configService.get<string>('MEDIAINFO_DIR'));
        const generalMITrack = videoMIInfo.media.track.find(s => s['@type'] === 'General');
        const videoMITrack = videoMIInfo.media.track.find(s => s['@type'] === 'Video');
        if (!videoMITrack)
          throw new Error('Failed to get encoded video info');
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

        const rcloneMoveArgs = this.createRcloneMoveArgs(`${parsedInput.dir}/${preparedVideoFileName}`,
          `${job.data.storage}:${job.data._id}/${streamId}`);
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
          await rcloneHelper.deletePath(rcloneConfig, rcloneDir, job.data.storage, `${job.data._id}/${streamId}`, (args => {
            this.logger.info('rclone ' + args.join(' '));
          }));
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

  private async splitAndEncodeVideo(options: EncodeVideoOptions, quality: number, perQualitySettings: IEncodingSetting, segmentDuration: number = 30, outputFileName: string) {
    const { inputFile, parsedInput, inputFileUrl, sourceInfo, advancedSettings = {}, codec, videoParams, job } = options;
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
      const segmentFileName = codec === VideoCodec.AV1 && sourceInfo.isHDR ? `${quality}_${i}.mkv` : `${quality}_${i}.mp4`;
      const segmentFileSubPath = `${SPLIT_SEGMENT_FOLDER}/${segmentFileName}`;
      // Wait until the primary transcoder is not busy
      while (true) {
        await this.transcoderApiService.checkAndWaitForTranscoderPriority();
        const startTime = i * segmentDuration;
        this.logger.info(`Segments ${i + 1}/${totalSegments}`);
        this.CanRetryEncoding = true;
        if (codec === VideoCodec.H264 || codec === VideoCodec.H265 || codec === VideoCodec.AV1) {
          const crfKey = codec === VideoCodec.AV1 ? 'cq' : 'crf';
          const videoArgs = this.createVideoEncodingArgs({
            inputFile: inputFileUrl || inputFile, parsedInput, codec, quality, videoParams,
            sourceInfo, crfKey, advancedSettings, encodingSetting: perQualitySettings, splitFrom: startTime.toString(),
            splitDuration: segmentDuration.toString(), segmentIndex: i, outputFileName: segmentFileSubPath
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
            } else if (e.code) {
              // Handle encoding error
              this.logger.info(`Received error ${e.code} from FFmpeg, retrying...`);
              await new Promise(r => setTimeout(r, 30_000));
              continue;
            }
            throw e;
          }
        } else {
          // Pass 1 params
          const videoPass1Args = this.createTwoPassesVideoEncodingArgs({
            inputFile: inputFileUrl || inputFile, parsedInput, codec, quality, videoParams,
            sourceInfo, crfKey: 'cq', advancedSettings, encodingSetting: perQualitySettings, pass: 1, splitFrom: startTime.toString(),
            splitDuration: segmentDuration.toString(), segmentIndex: i, outputFileName: segmentFileSubPath
          });
          // Pass 2 params
          const videoPass2Args = this.createTwoPassesVideoEncodingArgs({
            inputFile: inputFileUrl || inputFile, parsedInput, codec, quality, videoParams,
            sourceInfo, crfKey: 'cq', advancedSettings, encodingSetting: perQualitySettings, pass: 2, splitFrom: startTime.toString(),
            splitDuration: segmentDuration.toString(), segmentIndex: i, outputFileName: segmentFileSubPath
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
            } else if (e.code) {
              this.logger.info(`Received error ${e.code} from FFmpeg, retrying...`);
              await new Promise(r => setTimeout(r, 30_000));
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
    const concatSegmentArgs = this.createConcatSegmentArgs(concatSegmentFile, parsedInput, outputFileName);
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
    await rcloneHelper.refreshRemoteTokens(rcloneConfig, rcloneDir, remoteList, args => {
      this.logger.info('rclone ' + args.join(' '));
    });
  }

  private async prepareMediaFile(inputFileName: string, outputFileName: string, parsedInput: path.ParsedPath, tempFileName: string,
    playlistName: string, job: Job<IVideoData>) {
    this.logger.info(`Preparing media file: ${inputFileName}`);
    // Trim saved file name
    const trimmedFileName = job.data.linkedStorage ? stringHelper.trimSlugFilename(job.data.filename) : job.data.filename;
    const inputFilePath = `${parsedInput.dir}/${inputFileName}`;
    const outputFilePath = `${parsedInput.dir}/${outputFileName}`;
    const inputSourceFile = `${parsedInput.dir}/${trimmedFileName}`;
    const hasFreeSpace = await diskSpaceUtil.hasFreeSpaceToCopyFile(inputFilePath, parsedInput.dir);
    if (!hasFreeSpace) {
      this.logger.warning(`Not enough disk space to duplicate file, deleting: ${trimmedFileName} temporary`);
      await fileHelper.deleteFile(inputSourceFile);
    }
    const mp4boxPackArgs = this.createMP4BoxPackArgs(inputFilePath, parsedInput, tempFileName, playlistName);
    await this.packageMedia(mp4boxPackArgs, job.id);
    await fileHelper.deleteFile(inputFilePath);
    const tempFilePath = `${parsedInput.dir}/${tempFileName}.mp4`;
    await fileHelper.renameFile(tempFilePath, outputFilePath);
    if (!hasFreeSpace) {
      this.logger.info(`Redownloading: ${job.data.filename}`);
      const rcloneDir = this.configService.get<string>('RCLONE_DIR');
      const rcloneConfigFile = this.configService.get<string>('RCLONE_CONFIG_FILE');
      const downloadStorage = job.data.linkedStorage || job.data.storage;
      await rcloneHelper.downloadFile(rcloneConfigFile, rcloneDir, downloadStorage, job.data.path, job.data.filename,
        parsedInput.dir, !!job.data.linkedStorage,
        (args => {
          this.logger.info('rclone ' + args.join(' '));
        }));
      if (job.data.linkedStorage) {
        // Set trimmed file name
        await fileHelper.renameFile(`${parsedInput.dir}/${job.data.filename}`, inputSourceFile);
      }
    }
  }

  private async saveManifestFile(manifest: StreamManifest, transcodeDir: string, codec: number, job: Job<IVideoData>, sourceInfo?: VideoSourceInfo) {
    const manifestFileName = `manifest_${codec}.json`;
    const manifestFilePath = `${transcodeDir}/${manifestFileName}`;
    const streamId = await createSnowFlakeId();
    this.logger.info(`Generating manifest file: ${manifestFileName}`);
    await manifest.saveFile(manifestFilePath);
    const rcloneMoveManifestArgs = this.createRcloneMoveArgs(manifestFilePath, `${job.data.storage}:${job.data._id}/${streamId}`);
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

  private createAudioEncodingArgs(options: CreateAudioEncodingArgsOptions) {
    const { inputFile, parsedInput, audioParams, codec, channels, downmix, audioIndex, outputFileName } = options;
    const bitrate = AudioCodec.OPUS === codec ? 128 : AudioCodec.OPUS_SURROUND === codec ? 64 * channels : 0;
    const args: string[] = [
      '-hide_banner', '-y',
      '-progress', 'pipe:1',
      '-loglevel', 'error',
      '-i', `"${inputFile}"`,
      '-vn'
    ];
    if (this.UseURLInput) {
      args.push(
        '-reconnect', '1',
        '-reconnect_on_http_error', '400,401,403,408,409,429,5xx',
      );
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
      }
      else if (codec === AudioCodec.OPUS) {
        args.push('-ac', '2');
        args.push('-mapping_family', '0');
      }
    } else if (channels > 2) {
      const channelValue = channels <= 8 ? channels.toString() : '8'; // 8 channels (7.1) is the limit for both aac and opus
      args.push('-ac', channelValue);
      if (codec === AudioCodec.OPUS_SURROUND) {
        args.push('-mapping_family', '1');
      }
    }
    args.push(
      '-map', `0:${audioIndex}`,
      //'-map_metadata', '-1',
      '-map_chapters', '-1',
      '-f', 'mp4',
      `"${parsedInput.dir}/${outputFileName}"`
    );
    return args;
  }

  private createVideoEncodingArgs(options: CreateVideoEncodingArgsOptions) {
    const { inputFile, parsedInput, codec, quality, videoParams, sourceInfo, crfKey, advancedSettings, encodingSetting,
      splitFrom, splitDuration, outputFileName } = options;
    const gopSize = (sourceInfo.fps ? sourceInfo.fps * 2 : 48).toString();
    const bitDepth = codec === VideoCodec.H264 ? 8 : 10;
    const videoFilters = this.resolveVideoFilters({
      quality,
      hdrTonemap: codec === VideoCodec.H264 && sourceInfo.isHDR,
      bitDepth
    });
    const args: string[] = [
      '-hide_banner', '-y',
      '-hwaccel', 'auto',
      '-progress', 'pipe:1',
      '-loglevel', 'error'
    ];
    if (this.UseURLInput) {
      args.push(
        '-reconnect', '1',
        '-reconnect_on_http_error', '400,401,403,408,409,429,5xx',
      );
    }
    splitFrom && args.push('-ss', splitFrom);
    args.push('-i', `"${inputFile}"`);;
    splitDuration && args.push('-t', splitDuration);
    args.push(
      ...videoParams,
      '-g', gopSize,
      '-keyint_min', gopSize,
      '-sc_threshold', '0'
    );
    if (encodingSetting)
      this.resolveEncodingSettings(args, encodingSetting, sourceInfo, crfKey);
    if (codec === VideoCodec.H264)
      this.resolveH264Params(args, advancedSettings, quality, sourceInfo);
    else if (codec === VideoCodec.AV1)
      this.resolveSVTAV1Params(args, advancedSettings, sourceInfo);
    args.push(
      '-map', '0:v:0',
      //'-map_metadata', '-1',
      '-map_chapters', '-1',
      '-vf', videoFilters,
      //'-movflags', '+faststart',
      `"${parsedInput.dir}/${outputFileName}"`
    );
    return args;
  }

  private createTwoPassesVideoEncodingArgs(options: CreateVideoEncodingArgsOptions & { pass: number }) {
    const { inputFile, parsedInput, codec, quality, videoParams, sourceInfo, crfKey, advancedSettings, encodingSetting, pass,
      splitFrom, splitDuration, segmentIndex, outputFileName } = options;
    const gopSize = (sourceInfo.fps ? sourceInfo.fps * 2 : 48).toString();
    const bitDepth = codec === VideoCodec.H264 ? 8 : 10;
    const videoFilters = this.resolveVideoFilters({ quality, hdrTonemap: false, bitDepth });
    if (pass === 1) {
      const outputName = process.platform === 'win32' ? 'NUL' : '/dev/null';
      const args = [
        '-hide_banner', '-y',
        '-hwaccel', 'auto',
        '-progress', 'pipe:1',
        '-loglevel', 'error'
      ];
      if (this.UseURLInput) {
        args.push(
          '-reconnect', '1',
          '-reconnect_on_http_error', '400,401,403,408,409,429,5xx',
        );
      }
      splitFrom && args.push('-ss', splitFrom);
      args.push('-i', `"${inputFile}"`);
      splitDuration && args.push('-t', splitDuration);
      args.push(...videoParams,
        '-g', gopSize,
        '-keyint_min', gopSize,
        '-sc_threshold', '0'
      );
      if (encodingSetting)
        this.resolveEncodingSettings(args, encodingSetting, sourceInfo, crfKey);
      if (codec === VideoCodec.H264)
        this.resolveH264Params(args, advancedSettings, quality, sourceInfo);
      else if (codec === VideoCodec.AV1)
        this.resolveSVTAV1Params(args, advancedSettings, sourceInfo);
      args.push(
        '-map', '0:v:0',
        '-vf', videoFilters,
        //'-movflags', '+faststart'
      );
      if (segmentIndex != null) {
        args.push('-passlogfile', `"${parsedInput.dir}/${SPLIT_SEGMENT_FOLDER}/${parsedInput.name}_2pass.log"`);
      } else {
        args.push('-passlogfile', `"${parsedInput.dir}/${parsedInput.name}_2pass.log"`);
      }
      args.push(
        '-pass', '1', '-an',
        '-f', 'null', outputName
      );
      return args;
    }
    const args = [
      '-hide_banner', '-y',
      '-hwaccel', 'auto',
      '-progress', 'pipe:1',
      '-loglevel', 'error'
    ];
    if (this.UseURLInput) {
      args.push(
        '-reconnect', '1',
        '-reconnect_on_http_error', '400,401,403,408,409,429,5xx',
      );
    }
    splitFrom && args.push('-ss', splitFrom);
    args.push('-i', `"${inputFile}"`);
    splitDuration && args.push('-t', splitDuration);
    args.push(
      ...videoParams,
      '-g', gopSize,
      '-keyint_min', gopSize,
      '-sc_threshold', '0'
    );
    if (encodingSetting)
      this.resolveEncodingSettings(args, encodingSetting, sourceInfo, crfKey);
    if (codec === VideoCodec.H264)
      this.resolveH264Params(args, advancedSettings, quality, sourceInfo);
    else if (codec === VideoCodec.AV1)
      this.resolveSVTAV1Params(args, advancedSettings, sourceInfo);
    args.push(
      '-map', '0:v:0',
      //'-map_metadata', '-1',
      '-map_chapters', '-1',
      '-vf', videoFilters,
      //'-movflags', '+faststart'
    );
    if (segmentIndex != null) {
      args.push('-passlogfile', `"${parsedInput.dir}/${SPLIT_SEGMENT_FOLDER}/${parsedInput.name}_2pass.log"`);
    } else {
      args.push('-passlogfile', `"${parsedInput.dir}/${parsedInput.name}_2pass.log"`);
    }
    args.push(
      '-pass', '2',
      `"${parsedInput.dir}/${outputFileName}"`
    );
    return args;
  }

  private resolveEncodingSettings(args: string[], encodingSetting: IEncodingSetting, sourceInfo: VideoSourceInfo,
    crfKey: 'crf' | 'cq' = 'crf') {
    let crfValue = null;
    if (crfKey === 'crf')
      if (sourceInfo.codec === 'h265')
        crfValue = encodingSetting.h265Crf;
      else
        crfValue = encodingSetting.crf;
    else if (crfKey === 'cq')
      crfValue = encodingSetting.cq;
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

  private resolveH264Params(args: string[], advancedSettings: AdvancedVideoSettings, quality: number, sourceInfo: VideoSourceInfo) {
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
        'tune=0', 'enable-overlays=1', 'film-grain=0', 'film-grain-denoise=0', 'scd=1', 'sharpness=0', 'enable-qm=1', 'qm-min=0',
        'enable-variance-boost=1',
      ],
      psy: ['tune=0', 'enable-overlays=1', 'film-grain=0', 'film-grain-denoise=0', 'sharpness=0', 'scd=1'],
      hdr: ['sharpness=0']
    };
    const svtAV1Params = svtAv1Preset === 'psy' ? svtAV1PresetParams.psy : svtAv1Preset === 'hdr' ? svtAV1PresetParams.hdr : svtAV1PresetParams.main;
    if (advancedSettings.h264Tune !== 'animation')
      svtAV1Params.push('scm=0');
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
      videoFilters.push('zscale=t=linear:npl=100,format=gbrpf32le,tonemap=tonemap=mobius:desat=0,zscale=p=bt709:t=bt709:m=bt709:r=tv:d=error_diffusion');
      if (options.bitDepth === 10) {
        videoFilters.push('format=yuv420p10le');
      } else {
        videoFilters.push('format=yuv420p');
      }
    }
    return videoFilters.join(',');
  }

  private createConcatSegmentArgs(inputFile: string, parsedInput: path.ParsedPath, outputFile: string) {
    const args = [
      '-hide_banner', '-y',
      '-progress', 'pipe:1',
      '-loglevel', 'error',
      '-f', 'concat',
      '-safe', '0',
      '-i', `"${inputFile}"`,
      '-c', 'copy',
      `"${parsedInput.dir}/${outputFile}"`
    ];
    return args;
  }

  private createMP4BoxPackArgs(input: string, parsedInput: path.ParsedPath, tempFileName: string, playlistName: string) {
    const segmentInitName = process.platform === 'win32' ? '$Init=$' : '\\$Init=\\$';
    const args: string[] = [
      '-dash', '6000',
      '-profile', 'onDemand',
      '-segment-name', `"${tempFileName}${segmentInitName}"`,
      '-out', `"${parsedInput.dir}/${playlistName}:dual"`,
      `"${input}"`
    ];
    return args;
  }

  private createRcloneMoveArgs(source: string, dest: string, include?: string) {
    const rcloneConfigFile = this.configService.get<string>('RCLONE_CONFIG_FILE');
    const args: string[] = [
      '--config', rcloneConfigFile,
      '--low-level-retries', '5',
      '-v', '--use-json-log',
      '--stats', '3s',
      'move', `"${source}"`, `"${dest}"`
    ];
    if (include) {
      args.push('--include', include);
    }
    return args;
  }

  private createRcloneMoveThumbArgs(transcodeDir: string, remote: string, parentFolder: string, sync: boolean = false) {
    const targetCommand = sync ? 'sync' : 'move';
    const rcloneConfigFile = this.configService.get<string>('RCLONE_CONFIG_FILE');
    const args: string[] = [
      '--config', rcloneConfigFile,
      '--low-level-retries', '5',
      '-v', '--use-json-log',
      '--stats', '3s',
      targetCommand,
      `"${transcodeDir}/${this.thumbnailFolder}"`,
      `"${remote}:${parentFolder}/${this.thumbnailFolder}"`
    ];
    return args;
  }

  private encodeMedia(args: string[], videoDuration: number, jobId: string | number) {
    return new Promise<void>((resolve, reject) => {
      let isCancelled = false;
      let isRetryEncoding = false;
      let isProgressTimeout = false;
      let lastProgress: Progress | null = null;

      this.logger.info('ffmpeg ' + args.join(' '));
      const ffmpeg = child_process.spawn(`"${this.configService.get<string>('FFMPEG_DIR')}/ffmpeg"`, args, { shell: true });

      ffmpeg.stdout.setEncoding('utf8');
      ffmpeg.stdout.on('data', async (data: string) => {
        const progress = ffmpegHelper.parseProgress(data);
        if (!isEqualShallow(lastProgress, progress))
          isProgressTimeout = false;
        lastProgress = { ...progress };
        const percent = ffmpegHelper.progressPercent(progress.outTimeMs, videoDuration * 1000000);
        stdout.write(`${ffmpegHelper.getProgressMessage(progress, percent)}\r`);
      });

      ffmpeg.stderr.setEncoding('utf8');
      ffmpeg.stderr.on('data', (data) => {
        stdout.write(data);
      });

      const cancelledJobChecker = this.createCancelJobChecker(jobId, () => {
        isCancelled = true;
        ffmpeg.stdin.write('q');
        ffmpeg.stdin.end();
      });

      const retryEncodingChecker = this.createRetryEncodingChecker(() => {
        isRetryEncoding = true;
        ffmpeg.kill('SIGINT');
        ffmpeg.kill('SIGTERM');
      });

      const progressTimeoutChecker = this.createTimeoutChecker(() => {
        if (isProgressTimeout) {
          ffmpeg.kill('SIGINT');
          ffmpeg.kill('SIGTERM');
          return;
        }
        isProgressTimeout = true;
      });

      ffmpeg.on('exit', (code: number) => {
        stdout.write('\n');
        clearInterval(cancelledJobChecker);
        clearInterval(retryEncodingChecker);
        clearInterval(progressTimeoutChecker);
        if (isCancelled) {
          reject(RejectCode.JOB_CANCEL);
        } else if (isRetryEncoding) {
          reject(RejectCode.RETRY_ENCODING)
        } else if (isProgressTimeout) {
          reject(RejectCode.ENCODING_TIMEOUT)
        } else if (code !== 0) {
          reject({ code, message: `FFmpeg exited with status code: ${code}` });
        } else {
          resolve();
        }
      });
    });
  }

  private packageMedia(args: string[], jobId: string | number) {
    return new Promise<void>((resolve, reject) => {
      let isCancelled = false;

      this.logger.info('MP4Box ' + args.join(' '));
      const mp4box = child_process.spawn(`"${this.configService.get<string>('MP4BOX_DIR')}/MP4Box"`, args, { shell: true });

      mp4box.stderr.setEncoding('utf8');
      mp4box.stderr.on('data', (data) => {
        stdout.write(data);
      });

      const cancelledJobChecker = this.createCancelJobChecker(jobId, () => {
        isCancelled = true;
        mp4box.kill('SIGINT'); // Stop key
      });

      mp4box.on('exit', (code: number) => {
        stdout.write('\n');
        clearInterval(cancelledJobChecker);
        if (isCancelled) {
          reject(RejectCode.JOB_CANCEL);
        } else if (code !== 0) {
          reject(`MP4Box exited with status code: ${code}`);
        } else {
          resolve();
        }
      });
    });
  }

  private uploadMedia(args: string[], jobId: string | number) {
    return new Promise<void>((resolve, reject) => {
      let isCancelled = false;

      this.logger.info('rclone ' + args.join(' '));
      const rclone = child_process.spawn(`"${this.configService.get<string>('RCLONE_DIR')}/rclone"`, args, { shell: true });

      rclone.stderr.setEncoding('utf8');
      rclone.stderr.on('data', (data) => {
        const progress = rcloneHelper.parseRcloneUploadProgress(data);
        if (progress)
          stdout.write(`${progress.msg}\r`);
      });

      const cancelledJobChecker = this.createCancelJobChecker(jobId, () => {
        isCancelled = true;
        rclone.kill('SIGINT'); // Stop key
      });

      rclone.on('exit', (code: number) => {
        stdout.write('\n');
        clearInterval(cancelledJobChecker);
        if (isCancelled) {
          reject(RejectCode.JOB_CANCEL);
        } else if (code !== 0) {
          reject(`Rclone exited with status code: ${code}`);
        } else {
          resolve();
        }
      });
    });
  }

  private createCancelJobChecker(jobId: string | number, exec: () => void, ms: number = 5000) {
    return setInterval(() => {
      const index = this.CanceledJobIds.findIndex(j => +j === +jobId);
      if (index === -1) return;
      this.CanceledJobIds = this.CanceledJobIds.filter(id => +id > +jobId);
      // Exec callback
      exec();
    }, ms)
  }

  private createRetryEncodingChecker(exec: () => void, ms: number = 5000) {
    if (!this.CanRetryEncoding) return null;
    return setInterval(() => {
      if (!this.RetryEncoding) return;
      this.RetryEncoding = false;
      // Exec callback
      exec();
    }, ms)
  }

  private createTimeoutChecker(exec: () => void, ms: number = 600_000) {
    return setInterval(() => {
      exec();
    }, ms)
  }

  private findUploadedFiles(remote: string, parentFolder: string, jobId: string | number, exclude?: string) {
    const rcloneConfigFile = this.configService.get<string>('RCLONE_CONFIG_FILE');
    const args: string[] = [
      '--config', rcloneConfigFile,
      'lsjson', `${remote}:${parentFolder}`,
      '--recursive', '--files-only'
    ];
    if (exclude) {
      args.push('--exclude', exclude);
    }
    return new Promise<RcloneFile[]>((resolve, reject) => {
      let isCancelled = false;
      this.logger.info('rclone ' + args.join(' '));
      const rclone = child_process.spawn(`"${this.configService.get<string>('RCLONE_DIR')}/rclone"`, args, { shell: true });

      let listJson = '';

      rclone.stdout.setEncoding('utf8');
      rclone.stdout.on('data', (data) => {
        listJson += data;
      });

      rclone.stderr.setEncoding('utf8');
      rclone.stderr.on('data', (data) => {
        stdout.write(data);
      });

      const cancelledJobChecker = this.createCancelJobChecker(jobId, () => {
        isCancelled = true;
        rclone.kill('SIGINT');
      }, 500);

      rclone.on('exit', (code: number) => {
        clearInterval(cancelledJobChecker);
        if (isCancelled) {
          reject(RejectCode.JOB_CANCEL);
        } else if (code === 3) {
          // Return an empty array if directory not found
          resolve([]);
        } else if (code !== 0) {
          reject(`Error listing files, rclone exited with status code: ${code}`);
        } else {
          const fileData = JSON.parse(listJson);
          resolve(fileData);
        }
      });
    });
  }

  private async ensureRcloneConfigExist(configFile: string, storage: string, job: Job<IVideoData>) {
    const configExists = await fileHelper.findInFile(configFile, `[${storage}]`);
    if (!configExists) {
      this.logger.info(`Config for remote "${storage}" not found, generating...`);
      let externalStorage = await externalStorageModel.findOne({ _id: BigInt(storage) }).lean().exec();
      if (!externalStorage) {
        const statusError = await this.generateStatusError(StatusCode.STORAGE_NOT_FOUND, job);
        throw new Error(statusError.errorCode);
      }
      externalStorage = await this.decryptToken(externalStorage);
      const newConfig = rcloneHelper.createRcloneConfig(externalStorage);
      await fileHelper.appendToFile(configFile, newConfig);
      this.logger.info(`Generated config for remote "${storage}"`);
    }
  }

  private async getLinkedSourceUrl(job: Job<IVideoData>) {
    let externalStorage;
    if (job.data.linkedStorage)
      externalStorage = await externalStorageModel.findOne({ _id: BigInt(job.data.linkedStorage) }, { publicUrl: 1, folderId: 1 }).lean().exec();
    else
      externalStorage = await externalStorageModel.findOne({ _id: BigInt(job.data.storage) }, { publicUrl: 1, folderId: 1 }).lean().exec();
    if (!externalStorage) {
      const statusError = await this.generateStatusError(StatusCode.STORAGE_NOT_FOUND, job);
      throw new Error(statusError.errorCode);
    }
    if (!externalStorage.publicUrl)
      return null;
    const sourcePathItems = [externalStorage.folderId || '', job.data.path, job.data.filename];
    const sourcePath = path.posix.join(...sourcePathItems.map(value => encodeURIComponent(value)));
    return externalStorage.publicUrl.replace(':service_path', 's3').replace(':path', sourcePath);
  }

  private async findAvailableQuality(uploadedFiles: string[], allQualityList: number[], parsedInput: path.ParsedPath,
    codec: number, replaceStreams: string[] = [], job: Job<IVideoData>) {
    const fileIds: bigint[] = [];
    for (let i = 0; i < uploadedFiles.length; i++) {
      const uploadedFileName = uploadedFiles[i].split('/').pop();
      if (!allQualityList.find(q => uploadedFileName === `${parsedInput.name}_${q}.mp4`))
        continue;
      const stringId = uploadedFiles[i].split('/')[0];
      if (replaceStreams.includes(stringId))
        continue;
      if (isNaN(<any>stringId))
        continue;
      fileIds.push(BigInt(stringId));
    }
    await mongoose.connect(this.configService.get<string>('DATABASE_URL'), { family: 4, useBigInt64: true });
    const sourceFileMeta = await mediaStorageModel.findOne({ _id: BigInt(job.data._id) }).lean().exec();
    await mongoose.disconnect();
    const qualityList = sourceFileMeta.streams
      .filter(file => file.codec === codec && fileIds.includes(file._id))
      .map(file => file.quality);
    const availableQualityList = allQualityList.filter(quality => !qualityList.includes(quality));
    return availableQualityList;
  }

  private calculateQuality(height: number, qualityList: number[], forcedQualityList: number[] = [], fallbackQualityList: number[] = []) {
    const availableQualityList: number[] = [];
    if (!height) return availableQualityList;
    for (let i = 0; i < qualityList.length; i++) {
      if (height >= qualityList[i] || forcedQualityList.includes(qualityList[i])) {
        availableQualityList.push(qualityList[i]);
      }
    }
    // Use the lowest quality when there is no suitable one
    if (!availableQualityList.length)
      availableQualityList.push(...fallbackQualityList);
    return availableQualityList;
  }

  private async validateSourceQuality(options: ValidateSourceQualityOptions): Promise<number[] | null> {
    const { parsedInput, quality, qualityList, forcedQualityList, fallbackQualityList, codec, retryFromInterruption, job } = options;
    const allQualityList = this.calculateQuality(quality, qualityList, forcedQualityList, fallbackQualityList);
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
      const existingManifestData = await this.findExistingManifest(job.data.storage, job.data._id, codec);
      if (existingManifestData?.videoTracks)
        alreadyEncodedFiles = existingManifestData.videoTracks.map(t => t.uri);
      availableQualityList = await this.findAvailableQuality(alreadyEncodedFiles, allQualityList, parsedInput, codec,
        job.data.replaceStreams, job);
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
      await rcloneHelper.emptyPath(rcloneConfigFile, rcloneDir, job.data.storage, `${job.data._id}/*`, args => {
        this.logger.info('rclone ' + args.join(' '));
      }, {
        include: '*/**'
      });
    }
    return availableQualityList;
  }

  private async findExistingManifest(remote: string, parentFolder: string, codec: number) {
    const rcloneConfigFile = this.configService.get<string>('RCLONE_CONFIG_FILE');
    const rcloneDir = this.configService.get<string>('RCLONE_DIR');
    const isFolderExist = await rcloneHelper.isPathExist(rcloneConfigFile, rcloneDir, remote, parentFolder);
    if (!isFolderExist)
      return null;
    const [manifestFileInfo] = await rcloneHelper.listRemoteJson(rcloneConfigFile, rcloneDir, remote, parentFolder, {
      filesOnly: true,
      recursive: true,
      include: `*/manifest_${codec}.json`
    });
    if (!manifestFileInfo)
      return null;
    this.logger.info(`Found existing manifest from ${manifestFileInfo.Path}, reading data...`);
    const manifestContent = await rcloneHelper.readRemoteFile(rcloneConfigFile, rcloneDir, remote, parentFolder,
      manifestFileInfo.Path, args => {
        this.logger.info('rclone ' + args.join(' '));
      });
    if (!manifestContent)
      return null;
    return <HlsManifest>JSON.parse(manifestContent);
  }

  private async decryptToken(storage: IStorage) {
    const stringCrypto = new StringCrypto(this.configService.get<string>('CRYPTO_SECRET_KEY'));
    storage.clientSecret = await stringCrypto.decrypt(storage.clientSecret);
    return storage;
  }

  private async generateStatusError(errorCode: string, job: Job<IVideoData>, options: { discard: boolean } = { discard: false }) {
    const status = { errorCode, jobId: job.id, ...job.data };
    const statusJson = JSON.stringify(status);
    this.logger.error(`Error: ${errorCode} - ${statusJson}`);
    await this.daplexApiService.ensureProducerAppIsOnline(job.data.producerUrl);
    if (options.discard)
      job.discard();
    if (options.discard || job.attemptsMade >= job.opts.attempts)
      await this.videoResultQueue.add('failed-encoding', status);
    else if (job.attemptsMade < job.opts.attempts)
      await this.videoResultQueue.add('retry-encoding', status);
    return status;
  }

  private generateStatus(job: Job<IVideoData>) {
    return { jobId: job.id, ...job.data };
  }
}