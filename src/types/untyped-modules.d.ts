// Modules without bundled type declarations. The mongodb driver's internal BSON
// entry point is deep-imported to monkey-patch resolveBSONOptions; m3u8-parser
// ships an untyped CommonJS build. Declare both so imports type-check without an
// inline `any` cast on the import statement.
declare module 'mongodb/lib/bson.js';
declare module 'm3u8-parser';
