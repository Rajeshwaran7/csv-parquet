import { Readable } from "stream";
import { ParquetSchema } from "parquetjs-lite/lib/schema";

export interface ICsvToParquetFile {
  /**
   * Downloads a CSV file from Blob Storage, converts it to Parquet format, and uploads it back to Blob Storage.
   * @param {string} blobName The name of the blob (CSV file) to process.
   * @returns {Promise<void>}
   */
   onCSVUpdateAndSuccess(blobName: string): Promise<void>;

  /**
   * Uploads Parquet data to Blob Storage.
   * @param {Buffer} buffer The Parquet data.
   * @param {string} blobName The name of the blob (Parquet file) to upload.
   * @returns {Promise<void>}
   */
  uploadParquetToBlobStorage(buffer: Buffer, blobName: string): Promise<void>;

  /**
   * Converts CSV data to Parquet format.
   * @param {Readable} csvStream A readable stream containing CSV data.
   * @returns {Promise<Buffer>} The Parquet data as a Buffer.
   */
  convertCSVtoParquet(csvStream: Readable): Promise<Buffer>;

  /**
   * Infers the schema for the Parquet file based on the CSV headers and a sample row.
   * @param {string[]} headers The headers from the CSV file.
   * @param {any} sampleRow A sample row from the CSV file.
   * @returns {ParquetSchema} The inferred Parquet schema.
   */
  inferSchema(headers: string[], sampleRow: any): ParquetSchema;

  /**
   * Downloads a CSV file from Blob Storage.
   * @param {string} blobName The name of the blob (CSV file) to download.
   * @returns {Promise<Readable>} A readable stream containing the CSV data.
   */
  downloadCSVFromBlobStorage(blobName: string): Promise<Readable>;
}
