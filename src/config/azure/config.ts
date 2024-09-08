import { BlobServiceClient, newPipeline, StoragePipelineOptions, AnonymousCredential } from "@azure/storage-blob";
import * as dotenv from 'dotenv';
dotenv.config();

/**
 * Configuration for Azure Storage operations
 *
 * @class AzureStorageConfig
 */
class AzureStorageConfig {
  private readonly containerName: string;
  private readonly targetConnectionString: string;
  private readonly blobServiceClient: BlobServiceClient;
  private readonly containerClient: any; // Consider defining a more specific type if possible
  private readonly sasUrl: string;
  private readonly proxyPort: string;
  private readonly zenProxy: string;
  private readonly proxyOptions: { host: string; port: string; } | any;

  public constructor() {
    this.containerName =  this.getEnvVariable('CONTAINER');
    this.targetConnectionString = this.getEnvVariable('TARGET_CONNECTION_CONTAINER');
    this.sasUrl = this.getEnvVariable('SAS_KEY_UPLOAD_PARQUET')
    this.proxyPort = this.getEnvVariable('PROXY_SAS_PORT')
    this.zenProxy = this.getEnvVariable('ZEN_PROXY')

    if (!this.containerName || !this.targetConnectionString || !this.sasUrl || !this.proxyPort || !this.zenProxy) {
      throw new Error('One or more required environment variables are missing.');
    }

    this.blobServiceClient = BlobServiceClient.fromConnectionString(this.targetConnectionString);
    this.containerClient = this.blobServiceClient.getContainerClient(this.containerName);

    this.proxyOptions = {
      host: this.zenProxy,
      port: this.proxyPort,
    };
  }


  private getEnvVariable(name: string): string {
    const value = process.env[name];
    console.log(value);
    
    if (!value) {
      throw new Error(`Environment variable ${name} is missing`);
    }
    return value;
  }


  public getBlobServiceClient(): BlobServiceClient {
    return this.blobServiceClient;
  }

  public getContainerClient(): any { // Consider defining a more specific type if possible
    return this.containerClient;
  }

  public getSasUrl(): string {
    return this.sasUrl;
  }

  public getProxyOptions(): { host: string; port: string; } {
    return this.proxyOptions;
  }

  public getPipeline() {
    const pipelineOptions: StoragePipelineOptions = { proxyOptions: this.proxyOptions };
    const anonymousCredential = new AnonymousCredential();
    return newPipeline(anonymousCredential, pipelineOptions);
  }
}

export default new AzureStorageConfig();
