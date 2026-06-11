import { Schema } from 'mongoose';

export interface IMedia {
  _id: bigint;
  type: string;
  originalLang: string;
}

export const mediaSchema = new Schema<IMedia>({
  _id: { type: Schema.Types.Mixed, required: true },
  type: { type: String, required: true },
  originalLang: { type: String }
});
