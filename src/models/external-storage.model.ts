import { Schema } from 'mongoose';

export interface IExternalStorage {
  _id: bigint;
  name: string;
  kind: number;
  clientId: string;
  clientSecret: string;
  accessToken: string;
  refreshToken: string;
  expiry: Date;
  folderId: string;
  folderName: string;
  publicUrl: string;
  secondPublicUrl: string;
  inStorage: string;
  used: number;
  files: bigint[];
}

export const externalStorageSchema = new Schema<IExternalStorage>({
  _id: { type: Schema.Types.Mixed, required: true },
  name: { type: String, required: true, unique: true },
  kind: { type: Number, required: true },
  accessToken: { type: String },
  clientId: { type: String, required: true },
  clientSecret: { type: String, required: true },
  refreshToken: { type: String, required: true },
  expiry: { type: Date },
  folderId: { type: String },
  folderName: { type: String },
  publicUrl: { type: String },
  secondPublicUrl: { type: String },
  inStorage: { type: String },
  used: { type: Number, default: 0 },
  files: { type: [Schema.Types.Mixed] }
});
