import cron from "node-cron";
import { Readable } from "stream";
import AzureStorageConfig from "../config/azure/config";
import { ParquetWriter } from "parquetjs-lite";
import { parse } from "fast-csv";
import { MemoryWritable } from "../utils/memorywritable";
import { BlobServiceClient } from "@azure/storage-blob";
import { ParquetSchema } from "parquetjs-lite/lib/schema";
import { ICsvToParquetFile } from "../interfaces/icron/csv-parquet.interface";

/**
 *
 *
 * @export
 * @class CsvToParquetFile
 * @implements {ICsvToParquetFile}
 */
export class CsvToParquetFile implements ICsvToParquetFile{
  public constructor() {
    cron.schedule("*/5 * * * * *", async () => {
      console.log("Cron Start");
      await this.onCSVUpdateAndSuccess("blobName");
    });
  }

  /**
   *
   * Download CSV From BlobStorage
   * @param {string} blobName
   * @return  {Promise<string>}
   * @memberof CodeGen
   */
  public async onCSVUpdateAndSuccess(blobName: string): Promise<void> {
    try {
      const csvData = await this.downloadCSVFromBlobStorage(blobName);
      const parquetBuffer = await this.convertCSVtoParquet(csvData);
      const parquetBlobName = `${blobName.replace(".csv", ".parquet")}`;
      await this.uploadParquetToBlobStorage(parquetBuffer, parquetBlobName);
    } catch (error) {
      console.error("Error:", error);
    }
  }

  /**
   * Upload Parquet data to Blob Storage
   * @param {string} blobName Blob name for the Parquet file
   * @param {Buffer} parquetData Parquet data
   * @returns {Promise<void>}
   */
  public async uploadParquetToBlobStorage(
    buffer: Buffer,
    blobName: string
  ): Promise<void> {
    const pipeline = AzureStorageConfig.getPipeline();
    const blobServiceClients = new BlobServiceClient(
      AzureStorageConfig.getSasUrl(),
      pipeline
    );
    const containerClients = blobServiceClients.getContainerClient(
      "blob-container-name"
    );
    const blockBlobClients = containerClients.getBlockBlobClient(blobName);
    await blockBlobClients.uploadData(buffer);
  }

  /**
   * Converts the csv to parquet format
   * @param {string} csvData
   * @return {Promise<Buffer>}
   * @memberof CodeGen
   */
  public async convertCSVtoParquet(csvStream: Readable): Promise<Buffer> {
    let schema: ParquetSchema;
    let csvHeaders: string[] = [];
    const rows: any[] = [];

    return new Promise((resolve, reject) => {
      csvStream
        .pipe(parse({ headers: true }))
        .on("headers", (headers) => {
          csvHeaders = headers;
        })
        .on("data", (data) => {
          rows.push(data);
        })
        .on("end", async () => {
          try {
            if (rows.length > 0) {
              schema = this.inferSchema(csvHeaders, rows[0]);
              const memoryWritable = new MemoryWritable();
              const writer = await ParquetWriter.openStream(
                schema,
                memoryWritable
              );
              for (const row of rows) {
                await writer.appendRow(row);
              }

              await writer.close();
              const parquetBuffer = memoryWritable.getBuffer();

              resolve(parquetBuffer);
            } else {
              reject(new Error("No data to process"));
            }
          } catch (error) {
            console.error("Error processing rows:", error);
            reject(error);
          }
        })
        .on("error", (error) => {
          console.error("Error processing CSV:", error);
          reject(error);
        });
    });
  }

  /**
   *
   * This function allows you to configure the configuration of the CSV file with the given options and callback function that will be called when the configuration
   * @public
   * @param {string[]} headers
   * @return {*}
   * @memberof CodeGen
   */
  public inferSchema(headers: string[], sampleRow: any): ParquetSchema {
    const fields: any = {};
    headers.forEach((header) => {
      const sampleValue = sampleRow[header];
      let fieldType: any = {
        type: "UTF8",
        encoding: "PLAIN",
        compression: "SNAPPY",
      };

      if (sampleValue) {
        if (!isNaN(Number(sampleValue))) {
          fieldType = {
            type: "DOUBLE",
            encoding: "PLAIN",
            compression: "SNAPPY",
          };
        } else if (new Date(sampleValue).toString() !== "Invalid Date") {
          fieldType = {
            type: "TIMESTAMP_MILLIS",
            encoding: "PLAIN",
            compression: "SNAPPY",
          };
        }
      }

      fields[header] = fieldType;
    });

    return new ParquetSchema(fields);
  }

  /**
   *
   * Download CSV From BlobStorage
   * @param {string} blobName
   * @return  {Promise<string>}
   * @memberof CodeGen
   */
  public async downloadCSVFromBlobStorage(
    blobName: string
  ): Promise<Readable> {
    const blobClient =
      AzureStorageConfig.getContainerClient().getBlobClient(blobName);
    const downloadBlockBlobResponse = await blobClient.download();
    return downloadBlockBlobResponse.readableStreamBody as Readable;
  }
}
