import { S3Locator } from "@mcma/aws-s3";
import { S3 } from "aws-sdk";

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

export async function writeOutputFile(filename: string, contents: any, s3: S3): Promise<S3Locator> {
    const outputFile = new S3Locator({
        url: s3.getSignedUrl("getObject", {
            Bucket: OUTPUT_BUCKET,
            Key: filename,
            Expires: 12 * 3600
        })
    });

    await s3.putObject({
        Bucket: outputFile.bucket,
        Key: outputFile.key,
        Body: (typeof contents === "string") ? contents : JSON.stringify(contents)
    }).promise();

    return outputFile;
}
