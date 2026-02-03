#!/usr/bin/env node
import { exec } from "child_process";
import { promisify } from "util";
import fs from "fs";
import fsPromises from "fs/promises";
import path from "path";
import dotenv from 'dotenv';

import {
  S3Client,
  PutObjectCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
} from "@aws-sdk/client-s3";

const execAsync = promisify(exec);

// Load .env if exists
const envPath = path.resolve(process.cwd(), '.env');
if (fs.existsSync(envPath)) {
  dotenv.config({ path: envPath });
  console.log('.env file loaded');
} else {
  dotenv.config();
  console.log('.env file not found, using environment variables');
}

/* ================== FLAGS ================== */

const DRY_RUN = process.argv.includes("--dry-run");

/* ================== CONFIG ================== */

const PGDUMP_DIR = process.env.PGDUMP_DIR || "./backups/postgres";
const S3_BUCKET = process.env.S3_BUCKET || "my-postgres-backups";
const S3_PREFIX = process.env.S3_PREFIX || "pg";
const AWS_REGION = process.env.AWS_REGION || "eu-west-1";

const RETENTION_DAYS = 7;
const MIN_FILES = 7;

/* ============================================ */

function log(msg) {
  console.log(DRY_RUN ? `[DRY-RUN] ${msg}` : `[INFO] ${msg}`);
}

fs.mkdirSync(PGDUMP_DIR, { recursive: true });

const timestamp = new Date()
  .toISOString()
  .replace(/T/, "-")
  .replace(/:/g, "-")
  .slice(0, 16);

const compress =
  process.argv.find(a => a.startsWith("--compress="))?.split("=")[1]
  || "gzip";

let compressCmd;
let ext;

if (compress === "zstd") {
  compressCmd = "zstd -15 -T0";
  ext = "zst";
} else {
  compressCmd = "gzip -5";
  ext = "gz";
}

const filename = `pg_dumpall-${timestamp}.sql.${ext}`;
const filepath = path.join(PGDUMP_DIR, filename);
const s3Key = `${S3_PREFIX}/${filename}`;

const s3 = new S3Client({
  region: AWS_REGION,
  credentials: process.env.AWS_ACCESS_KEY_ID
    ? {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      }
    : undefined,
});

async function createBackup() {
  log("Creating PostgresSQL backup");

  const pgDumpallCmd = `
  set -euo pipefail
  
  pg_dumpall --clean --if-exists --load-via-partition-root --quote-all-identifiers --no-password`;

  console.log(`  ${pgDumpallCmd} | ${compressCmd} > ${filepath}`);

  const env = { ...process.env };

  const tmpFilepath = `${filepath}.tmp`;

  const cmd = `${pgDumpallCmd} | ${compressCmd} > ${tmpFilepath}`;

  const {
    stdout,
    stderr,
  } = await execAsync(cmd, { env });

  if (stdout) console.log('stdout:', stdout);
  if (stderr) console.log('stderr:', stderr);

  if (!fs.existsSync(tmpFilepath)) {
    throw new Error(`Temporary backup file not found: ${tmpFilepath}`);
  }

  log(`Moving temporary file to final location: ${filepath}`);
  await fsPromises.rename(tmpFilepath, filepath);
  log(`Backup file created: ${filepath}`);
}

async function uploadToS3() {
  log(`Uploading to S3: s3://${S3_BUCKET}/${s3Key}`);

  if (DRY_RUN) return;

  if (!fs.existsSync(filepath)) {
    throw new Error(`Backup file not found: ${filepath}`);
  }

  const stream = fs.createReadStream(filepath);

  await s3.send(
    new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: s3Key,
      Body: stream,
    })
  );
}

async function cleanupLocal() {
  log(`Deleting local file: ${filepath}`);

  if (DRY_RUN) {
    log("Local file deletion skipped (dry-run)");
    return;
  }

  if (!fs.existsSync(filepath)) {
    log("Local file not found (already deleted or never created)");
    return;
  }

  try {
    await fsPromises.unlink(filepath);
    log("Local file deleted successfully");
  } catch (err) {
    throw new Error(`Failed to delete local file: ${err.message}`);
  }
}

async function cleanupS3() {
  log("Checking S3 retention policy");

  const listed = await s3.send(
    new ListObjectsV2Command({
      Bucket: S3_BUCKET,
      Prefix: `${S3_PREFIX}/`,
    })
  );

  const objects = listed.Contents || [];

  log(`Found ${objects.length} backups in S3`);

  if (objects.length <= MIN_FILES) {
    log("Retention skipped (minimum files threshold)");
    return;
  }

  const cutoff = Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000;

  const expired = objects.filter(
    (o) => o.LastModified && o.LastModified.getTime() < cutoff
  );

  if (expired.length === 0) {
    log("No expired backups found");
    return;
  }

  log(`Expired backups: ${expired.length}`);

  expired.forEach((o) =>
    console.log(`  - ${o.Key} (${o.LastModified.toISOString()})`)
  );

  if (DRY_RUN) {
    log("Deletion skipped (dry-run)");
    return;
  }

  await s3.send(
    new DeleteObjectsCommand({
      Bucket: S3_BUCKET,
      Delete: {
        Objects: expired.map((o) => ({ Key: o.Key })),
      },
    })
  );

  log("Expired backups deleted");
}

async function main() {
  try {
    log(`Starting backup${DRY_RUN ? " (dry-run)" : ""}`);

    await createBackup();
    await uploadToS3();
    await cleanupLocal();
    await cleanupS3();

    log("Backup flow completed");
  } catch (err) {
    console.error("[ERROR] Backup failed");
    console.error(err);
    process.exit(1);
  }
}

main();
