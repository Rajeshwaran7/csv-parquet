import { Writable } from "stream";

/**
 *
 *
 * @export
 * @class MemoryWritable
 * @extends {Writable}
 */
export class MemoryWritable extends Writable {
  private chunks: Buffer[];

  constructor() {
    super();
    this.chunks = [];
  }
  /**
   * This is the write method
   *
   * @param {Buffer} chunk
   * @param {string} encoding
   * @param {Function} callback
   * @memberof MemoryWritable
   */
  _write(chunk: Buffer, encoding: string, callback: () => void) {
    this.chunks.push(chunk);
    callback();
  }
  /**
   * This is the read
   *
   * @return {*}  {Buffer}
   * @memberof MemoryWritable
   */
  getBuffer(): Buffer {
    return Buffer.concat(this.chunks);
  }
}
