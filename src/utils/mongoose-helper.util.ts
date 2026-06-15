import { BSONSerializeOptions } from 'mongodb';
import * as mongoBson from 'mongodb/lib/bson.js';

export class MongooseHelper {
  applyMongoDBPatches() {
    (BigInt.prototype as any)['toJSON'] = function () {
      return this.toString();
    };
    mongoBson['resolveBSONOptions'] = function (
      options?: BSONSerializeOptions,
      parent?: { bsonOptions?: BSONSerializeOptions }
    ): BSONSerializeOptions {
      const parentOptions = parent?.bsonOptions;
      return {
        raw: options?.raw ?? parentOptions?.raw ?? false,
        useBigInt64: options?.useBigInt64 ?? parentOptions?.useBigInt64 ?? true,
        promoteLongs: options?.promoteLongs ?? parentOptions?.promoteLongs ?? true,
        promoteValues: options?.promoteValues ?? parentOptions?.promoteValues ?? true,
        promoteBuffers: options?.promoteBuffers ?? parentOptions?.promoteBuffers ?? false,
        ignoreUndefined: options?.ignoreUndefined ?? parentOptions?.ignoreUndefined ?? false,
        bsonRegExp: options?.bsonRegExp ?? parentOptions?.bsonRegExp ?? false,
        serializeFunctions: options?.serializeFunctions ?? parentOptions?.serializeFunctions ?? false,
        fieldsAsRaw: options?.fieldsAsRaw ?? parentOptions?.fieldsAsRaw ?? {},
        enableUtf8Validation: options?.enableUtf8Validation ?? parentOptions?.enableUtf8Validation ?? true
      };
    };
  }

  applyBigIntPatches() {
    (BigInt.prototype as any)['toJSON'] = function () {
      return this.toString();
    };
  }
}

export const mongooseHelper = new MongooseHelper();
