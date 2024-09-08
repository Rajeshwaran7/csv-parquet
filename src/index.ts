import express, { Application, Request, Response } from "express";
import { CsvToParquetFile } from './csv-parquet-job/csv-parquet.corn';
import * as dotenv from 'dotenv';
dotenv.config();

const app: Application = express();
const port = 3000;

app.use(express.json()); // Middleware for parsing JSON bodies


app.get("/", (req: Request, res: Response) => {
  res.send("Welcome to the Express TypeScript REST API");
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
  new CsvToParquetFile(); 

});

