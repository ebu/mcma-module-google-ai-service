import { GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { S3Locator } from "@mcma/aws-s3";

const { OUTPUT_BUCKET, OUTPUT_BUCKET_PREFIX } = process.env;

export function generateFilePrefix(url: string) {
    let filename = decodeURIComponent(new URL(url).pathname);
    let pos = filename.lastIndexOf("/");
    if (pos >= 0) {
        filename = filename.substring(pos + 1);
    }
    pos = filename.lastIndexOf(".");
    if (pos >= 0) {
        filename = filename.substring(0, pos);
    }

    return `${OUTPUT_BUCKET_PREFIX}${new Date().toISOString().substring(0, 19).replace(/[:]/g, "-")}/${filename}`;
}

export function getFileExtension(url: string, withDot: boolean = true) {
    let filename = decodeURIComponent(new URL(url).pathname);
    let pos = filename.lastIndexOf("/");
    if (pos >= 0) {
        filename = filename.substring(pos + 1);
    }
    pos = filename.lastIndexOf(".");
    if (pos >= 0) {
        return filename.substring(pos + (withDot ? 0 : 1));
    }
    return "";
}

export async function writeOutputFile(objectKey: string, contents: any, s3Client: S3Client): Promise<S3Locator> {
    await s3Client.send(new PutObjectCommand({
        Bucket: OUTPUT_BUCKET,
        Key: objectKey,
        Body: (typeof contents === "string") ? contents : JSON.stringify(contents),
    }));

    const command = new GetObjectCommand({
        Bucket: OUTPUT_BUCKET,
        Key: objectKey,
    });

    return new S3Locator({ url: await getSignedUrl(s3Client, command, { expiresIn: 12 * 3600 }) });
}
