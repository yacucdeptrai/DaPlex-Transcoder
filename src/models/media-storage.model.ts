import { Schema } from 'mongoose';

import { IMediaStorageStream, mediaStorageStreamSchema } from './media-storage-stream.model';

export interface IMediaStorage {
  _id: bigint;
  type: number;
  name: string;
  path: string;
  quality: number;
  mimeType: string;
  size: number;
  streams: IMediaStorageStream[];
  media: bigint;
  episode: bigint;
  storage: bigint;
}

export const mediaStorageSchema = new Schema<IMediaStorage>({
  _id: { type: Schema.Types.Mixed, required: true },
  type: { type: Number, required: true },
  name: { type: String, required: true },
  path: { type: String, required: true },
  quality: { type: Number, required: true },
  mimeType: { type: String, required: true },
  size: { type: Number, required: true },
  streams: { type: [mediaStorageStreamSchema], default: [] },
  media: { type: Schema.Types.Mixed, required: true },
  episode: { type: Schema.Types.Mixed },
  storage: { type: Schema.Types.Mixed, required: true }
});
