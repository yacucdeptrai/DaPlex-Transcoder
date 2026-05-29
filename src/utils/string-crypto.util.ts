import * as crypto from 'crypto';

export class StringCrypto {
  algorithm: string = 'aes256';
  key: string;

  constructor(secretKey: string) {
    this.key = crypto.createHash('sha256').update(secretKey).digest('base64').substr(0, 32);
  }

  encrypt(text: string) {
    return new Promise<string>((resolve, reject) => {
      if (!this.key)
        reject('Encrypt failed: Crypto key is missing');
      if (!text)
        resolve(null);
      // Generate a fresh IV per call. Reusing a single IV across encryptions in CBC
      // mode leaks information about identical plaintext prefixes. The IV is appended
      // to the ciphertext (after '.') so decrypt() can recover it.
      const iv = crypto.randomBytes(16);
      const cipher = crypto.createCipheriv(this.algorithm, this.key, iv);
      const encrypted = cipher.update(text, 'utf-8', 'base64') + cipher.final('base64');
      resolve(encrypted + '.' + iv.toString('base64'));
    });
  }

  decrypt(text: string) {
    return new Promise<string>((resolve, reject) => {
      if (!this.key)
        reject('Decrypt failed: Crypto key is missing');
      if (!text)
        resolve(null);
      const subText = text.split('.');
      if (subText.length !== 2)
        reject('Decrypt failed: Invalid input');
      const decipher = crypto.createDecipheriv(this.algorithm, this.key, Buffer.from(subText[1], 'base64'));
      const decrypted = decipher.update(subText[0], 'base64', 'utf-8') + decipher.final('utf-8');
      resolve(decrypted);
    });
  }
}