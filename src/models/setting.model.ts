import { Schema } from 'mongoose';

export interface ISetting {
  _id: bigint;
  defaultVideoCodecs: number;
  audioParams: string;
  audioSpeedParams: string;
  audioSurroundParams: string;
  audioSurroundOpusParams: string;
  videoH264Params: string;
  videoH265Params: string;
  videoVP9Params: string;
  videoAV1Params: string;
  videoQualityList: number[];
  videoNextGenQualityList: number[];
  videoEncodingSettings: IEncodingSetting[];
}

export interface IEncodingSetting {
  quality: number;
  crf: number;
  h265Crf: number;
  cq: number;
  maxrate: number;
  bufsize: number;
  useLowerRate: boolean;
}

const encodingSettingSchema = new Schema<IEncodingSetting>({
  quality: { type: Number, required: true },
  crf: { type: Number },
  cq: { type: Number },
  maxrate: { type: Number },
  bufsize: { type: Number },
  useLowerRate: { type: Boolean }
});

export const settingSchema = new Schema<ISetting>({
  _id: { type: Schema.Types.Mixed, required: true },
  defaultVideoCodecs: { type: Number },
  audioParams: { type: String },
  audioSpeedParams: { type: String },
  audioSurroundParams: { type: String },
  audioSurroundOpusParams: { type: String },
  videoH264Params: { type: String },
  videoH265Params: { type: String },
  videoVP9Params: { type: String },
  videoAV1Params: { type: String },
  videoQualityList: { type: [Number] },
  videoNextGenQualityList: { type: [Number] },
  videoEncodingSettings: { type: [encodingSettingSchema] }
});
