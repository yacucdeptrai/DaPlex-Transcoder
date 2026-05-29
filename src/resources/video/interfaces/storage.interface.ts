export interface IStorage {
  _id: bigint;
  name: string;
  clientId: string;
  clientSecret: string;
  refreshToken: string;
  accessToken: string;
  expiry: Date;
  folderId: string;
  kind: number;
  folderName: string;
  publicUrl: string;
  secondPublicUrl: string;
  inStorage: string;
  used: number;
  files: bigint[];
}