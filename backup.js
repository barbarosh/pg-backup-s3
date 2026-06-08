#!/usr/bin/env node
import { exec } from "child_process";
import { promisify } from "util";
import fs from "fs";
import fsPromises from "fs/promises";
import path from "path";
import dotenv from 'dotenv';

import {
  S3Client,
  ListObjectsV2Command,
  DeleteObjectsCommand,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";

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

const MIN_BACKUP_DAYS = getIntEnv("MIN_BACKUP_DAYS") ?? 7;
const S3_MIN_PART_SIZE = 5 * 1024 * 1024;
const S3_MULTIPART_PART_SIZE =
  (getIntEnv("S3_MULTIPART_PART_SIZE_MB") ?? 64) * 1024 * 1024;
const S3_MULTIPART_QUEUE_SIZE = getIntEnv("S3_MULTIPART_QUEUE_SIZE") ?? 4;

if (S3_MULTIPART_PART_SIZE < S3_MIN_PART_SIZE) {
  throw new Error("Invalid S3_MULTIPART_PART_SIZE_MB: must be at least 5");
}

if (S3_MULTIPART_QUEUE_SIZE < 1) {
  throw new Error("Invalid S3_MULTIPART_QUEUE_SIZE: must be at least 1");
}

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

function getIntEnv(name) {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return undefined;
  const value = Number.parseInt(raw, 10);
  if (!Number.isFinite(value)) {
    throw new Error(`Invalid ${name}: "${raw}" (must be an integer)`);
  }
  return value;
}

let compressCmd;
let ext;

if (compress === "zstd") {
  const level = getIntEnv("ZSTD_LEVEL") ?? getIntEnv("COMPRESSION_LEVEL") ?? 15;
  if (level < 1 || level > 19) {
    throw new Error(`Invalid zstd compression level: ${level} (expected 1..19)`);
  }
  compressCmd = `zstd -${level} -T0`;
  ext = "zst";
} else {
  const level = getIntEnv("GZIP_LEVEL") ?? getIntEnv("COMPRESSION_LEVEL") ?? 5;
  if (level < 1 || level > 9) {
    throw new Error(`Invalid gzip compression level: ${level} (expected 1..9)`);
  }
  compressCmd = `gzip -${level}`;
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

  if (DRY_RUN) return;

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

  const { size } = await fsPromises.stat(filepath);
  const stream = fs.createReadStream(filepath);
  const upload = new Upload({
    client: s3,
    params: {
      Bucket: S3_BUCKET,
      Key: s3Key,
      Body: stream,
    },
    partSize: S3_MULTIPART_PART_SIZE,
    queueSize: S3_MULTIPART_QUEUE_SIZE,
    leavePartsOnError: false,
  });

  upload.on("httpUploadProgress", (progress) => {
    if (progress.loaded && progress.total) {
      log(`Uploaded ${progress.loaded}/${progress.total} bytes to S3`);
    }
  });

  log(
    `Upload size: ${size} bytes; multipart part size: ${S3_MULTIPART_PART_SIZE} bytes`
  );
  await upload.done();
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

  const backups = objects
    .map((object) => {
      if (!object.LastModified) return null;

      const day = object.LastModified.toISOString().slice(0, 10);
      return {
        object,
        date: object.LastModified,
        day,
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.date.getTime() - a.date.getTime());
  const skippedObjects = objects.length - backups.length;

  if (skippedObjects > 0) {
    log(`Skipped objects without LastModified metadata: ${skippedObjects}`);
  }

  const protectedDays = new Set();

  for (const backup of backups) {
    if (protectedDays.size >= MIN_BACKUP_DAYS) break;
    protectedDays.add(backup.day);
  }

  const retentionCandidates = backups.filter(
    (backup) => !protectedDays.has(backup.day)
  );

  if (retentionCandidates.length === 0) {
    log("Retention skipped (minimum unique backup days threshold)");
    return;
  }

  log(`Protected latest unique backup days: ${protectedDays.size}`);

  const deletable = retentionCandidates;

  log(`Backups outside protected unique days: ${deletable.length}`);

  deletable.forEach((o) =>
    console.log(`  - ${o.object.Key} (${o.date.toISOString()})`)
  );

  if (DRY_RUN) {
    log("Deletion skipped (dry-run)");
    return;
  }

  await s3.send(
    new DeleteObjectsCommand({
      Bucket: S3_BUCKET,
      Delete: {
        Objects: deletable.map((o) => ({ Key: o.object.Key })),
      },
    })
  );

  log("Backups outside protected unique days deleted");
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
