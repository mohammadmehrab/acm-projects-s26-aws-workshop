import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client({ region: process.env.AWS_REGION });
const bucket = process.env.BUCKET_NAME;

const ALLOWED_TYPES = new Set(["image/jpeg", "image/png", "image/webp"]);
const MAX_BYTES = 10 * 1024 * 1024;
const KEY_PREFIX = process.env.KEY_PREFIX || "uploads/";

function corsHeaders(extra = {}) {
  return {
    "access-control-allow-origin": process.env.CORS_ORIGIN || "*",
    "access-control-allow-headers": "content-type",
    "access-control-allow-methods": "OPTIONS,POST",
    ...extra,
  };
}

function response(statusCode, bodyObj, extraHeaders = {}) {
  return {
    statusCode,
    headers: corsHeaders({
      "content-type": "application/json",
      ...extraHeaders,
    }),
    body: JSON.stringify(bodyObj),
  };
}

/** Safe S3 key suffix from query param (no path traversal or weird control chars). */
function safeObjectSuffix(raw) {
  if (!raw || typeof raw !== "string") return null;
  const base = decodeURIComponent(raw).split(/[/\\]/).pop().trim();
  if (!base || base.length > 200) return null;
  if (base.includes("..")) return null;
  if (/[\x00-\x1f\x7f]/.test(base)) return null;
  return base;
}

export async function handler(event) {
  if (!bucket) {
    console.error("Missing env BUCKET_NAME");
    return response(500, { message: "Server misconfiguration" });
  }

  const method = event.requestContext?.http?.method?.toUpperCase() || "";

  if (method === "OPTIONS") {
    return { statusCode: 204, headers: corsHeaders(), body: "" };
  }

  if (method !== "POST") {
    return response(405, { message: "Method not allowed" });
  }

  const suffix = safeObjectSuffix(event.queryStringParameters?.object);
  if (!suffix) {
    return response(400, {
      message: "Invalid or missing query param object",
    });
  }

  const headers = event.headers || {};
  const contentType = (
    headers["content-type"] ||
    headers["Content-Type"] ||
    ""
  )
    .split(";")[0]
    .trim()
    .toLowerCase();

  if (!ALLOWED_TYPES.has(contentType)) {
    return response(400, { message: "Content-Type must be image/jpeg, image/png, or image/webp" });
  }

  if (!event.body) {
    return response(400, { message: "Empty body" });
  }

  const body = Buffer.from(
    event.body,
    event.isBase64Encoded ? "base64" : "utf8"
  );

  if (body.length === 0) {
    return response(400, { message: "Empty body" });
  }

  if (body.length > MAX_BYTES) {
    return response(413, { message: "Body too large (max 10 MB)" });
  }

  const key = `${KEY_PREFIX}${suffix}`;

  try {
    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: body,
        ContentType: contentType,
      })
    );
  } catch (err) {
    console.error("PutObject failed", err);
    return response(500, { message: "Failed to store object", detail: err.message });
  }

  return response(200, { message: "OK", key });
}

