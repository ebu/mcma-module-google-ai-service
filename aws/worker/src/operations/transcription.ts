import { ProcessJobAssignmentHelper, ProviderCollection } from "@mcma/worker";
import { AIJob, ConfigVariables, McmaException, ProblemDetail, Utils } from "@mcma/core";
import { S3 } from "aws-sdk";
import { Storage, File } from "@google-cloud/storage";
import { v4 as uuidv4 } from "uuid";
import { default as axios } from "axios";

import * as speech from "@google-cloud/speech";

import { WorkerContext } from "../index";
import { generateFilePrefix, getFileExtension, writeOutputFile } from "./utils";
import { S3Locator } from "@mcma/aws-s3";
import { google } from "@google-cloud/speech/build/protos/protos";
import RecognitionConfig = google.cloud.speech.v1.RecognitionConfig;
import RecognitionAudio = google.cloud.speech.v1.RecognitionAudio;

const configVariables = ConfigVariables.getInstance();

export async function transcription(providers: ProviderCollection, jobAssignmentHelper: ProcessJobAssignmentHelper<AIJob>, ctx: WorkerContext) {
    const logger = jobAssignmentHelper.logger;
    const jobInput = jobAssignmentHelper.jobInput;

    logger.info("JobInput:");
    logger.info(jobInput);

    const inputFile = jobInput.inputFile as S3Locator;
    if (!inputFile.url || !Utils.isValidUrl(inputFile.url)) {
        await jobAssignmentHelper.fail(new ProblemDetail({
            type: "uri://mcma.ebu.ch/rfc7807/google-ai-service/locator-missing-url",
            title: "Provided input file locator is missing 'url' property"
        }));
        return;
    }

    let audioEncoding: google.cloud.speech.v1.RecognitionConfig.AudioEncoding;

    const inputFileExtension = getFileExtension(inputFile.url, false);
    switch (inputFileExtension.toLowerCase()) {
        case "flac":
            audioEncoding = google.cloud.speech.v1.RecognitionConfig.AudioEncoding.FLAC;
            break;
        case "wav":
            audioEncoding = google.cloud.speech.v1.RecognitionConfig.AudioEncoding.LINEAR16;
            break;
        case "ulaw":
            audioEncoding = google.cloud.speech.v1.RecognitionConfig.AudioEncoding.MULAW;
            break;
        case "amr":
        case "3ga":
            audioEncoding = google.cloud.speech.v1.RecognitionConfig.AudioEncoding.AMR;
            break;
        case "awb":
            audioEncoding = google.cloud.speech.v1.RecognitionConfig.AudioEncoding.AMR_WB;
            break;
        case "ogg":
        case "opus":
            audioEncoding = google.cloud.speech.v1.RecognitionConfig.AudioEncoding.OGG_OPUS;
            break;
        case "spx":
            audioEncoding = google.cloud.speech.v1.RecognitionConfig.AudioEncoding.SPEEX_WITH_HEADER_BYTE;
            break;
        case "webm":
            audioEncoding = google.cloud.speech.v1.RecognitionConfig.AudioEncoding.WEBM_OPUS;
            break;
        default:
            throw new McmaException(`Unsupported file format '${inputFileExtension}'`);

    }

    const googleCredentials = await getGoogleServiceCredentials(ctx.s3);

    const googleProjectId = googleCredentials.project_id;
    const googleClientEmail = googleCredentials.client_email;
    const googlePrivateKey = googleCredentials.private_key;

    logger.info("Project Id: " + googleProjectId);
    logger.info("Client Email: " + googleClientEmail);
    logger.info("Private Key Id: " + googleCredentials.private_key_id);

    const googleBucketName = configVariables.get("GoogleBucketName");
    const googleBucketLocation = configVariables.get("GoogleBucketLocation");
    logger.info(`Using google bucket with name '${googleBucketName}' in locatoin '${googleBucketLocation}`);

    const storage = new Storage({
        credentials: {
            client_email: googleClientEmail,
            private_key: googlePrivateKey,
        },
        projectId: googleProjectId
    });

    let bucketExists = false;
    const [buckets] = await storage.getBuckets();
    logger.info(buckets);

    for (let bucket of buckets) {
        if (bucket["id"] === googleBucketName) {
            bucketExists = true;
        }
    }

    if (!bucketExists) {
        await storage.createBucket(googleBucketName, {
            location: googleBucketLocation,
            standard: true
        });
        logger.info(`Bucket ${googleBucketName} created.`);
    } else {
        logger.info(`Bucket ${googleBucketName} already exists.`);
    }

    const googleBucket = storage.bucket(googleBucketName);

    const tempFileName = `${uuidv4()}${getFileExtension(inputFile.url, true)}`;

    const googleFile = googleBucket.file(tempFileName);

    const gsUri = await uploadUrlToGoogleBucket(inputFile.url, googleFile);

    try {

        logger.info(gsUri);

        const speechClient = new speech.SpeechClient({
            credentials: {
                client_email: googleClientEmail,
                private_key: googlePrivateKey,
            },
            projectId: googleProjectId
        });

        const languageCode = "en-US";
        const audioChannelCount = 2;

        const config = new RecognitionConfig({
            encoding: audioEncoding,
            audioChannelCount: audioChannelCount,
            languageCode: languageCode,
            enableAutomaticPunctuation: true,
            enableWordTimeOffsets: true,
        });

        const audio = new RecognitionAudio({
            uri: gsUri,
        });

        const request = {
            config: config,
            audio: audio,
        };

        const [operation] = await speechClient.longRunningRecognize(request);

        const [output] = await operation.promise();
        logger.info("Output:");
        logger.info(output);

        const jsonOutputFile = await writeOutputFile(generateFilePrefix(inputFile.url) + ".json", output, ctx.s3);

        const webvtt = generateWebVtt(output);
        logger.info(webvtt);

        const webVttOutputFile = await writeOutputFile(generateFilePrefix(inputFile.url) + ".vtt", webvtt, ctx.s3);

        logger.info("Updating job assignment with output");
        jobAssignmentHelper.jobOutput.outputFiles = [
            jsonOutputFile,
            webVttOutputFile,
        ];

        await jobAssignmentHelper.complete();
    } finally {
        try {
            logger.info("Removing file from Google Bucket " + googleBucketName);
            await googleFile.delete();
        } catch (error) {
            logger.error("Failed to delete file in Google Bucket " + googleBucketName);
            logger.error(error);
        }
    }
}

function generateWebVtt(output: any) {
    const MAX_CHARS_PER_LINE = 42;

    let webvtt = "WEBVTT";
    let index: number = 0;
    let start: number = -1;
    let end: number = -1;
    let sentence: string = "";

    for (const result of output.results) {
        for (const alternative of result.alternatives) {
            for (const word of alternative.words) {
                const testSentence = (sentence + " " + word.word).trim();

                if (sentence.endsWith(".") || sentence.endsWith("!") || sentence.endsWith("?") || testSentence.length > MAX_CHARS_PER_LINE) {
                    webvtt += `\n\n${index++}\n`;
                    webvtt += `${formatTimestamp(start)} --> ${formatTimestamp(end)}\n`;
                    webvtt += `${sentence}`;

                    start = -1;
                    end = -1;
                    sentence = word.word;
                } else {
                    sentence = testSentence;
                }

                if (start < 0) {
                    start = Number.parseInt(word.startTime.seconds ?? "0") + (word.startTime.nanos ?? 0) / 1000000000;
                }
                end = Number.parseInt(word.endTime.seconds ?? "0") + (word.endTime.nanos ?? 0) / 1000000000 - 0.001;
            }
        }
    }

    webvtt += `\n\n${index++}\n`;
    webvtt += `${formatTimestamp(start)} --> ${formatTimestamp(end)}\n`;
    webvtt += `${sentence}`;

    return webvtt;
}

function formatTimestamp(timestamp: number) {
    timestamp = Math.round(timestamp * 1000);
    const ms = timestamp % 1000;
    timestamp = Math.floor(timestamp / 1000);
    const s = timestamp % 60;
    timestamp = Math.floor(timestamp / 60);
    const m = timestamp % 60;
    timestamp = Math.floor(timestamp / 60);
    const h = timestamp;

    return `${h > 9 ? h : "0" + h}:${m > 9 ? m : "0" + m}:${s > 9 ? s : "0" + s}.${ms > 99 ? ms : ms > 9 ? "0" + ms : "00" + ms}`;
}

async function uploadUrlToGoogleBucket(url: string, googleFile: File): Promise<string> {
    return new Promise<string>(async (resolve, reject) => {
        const writeStream = googleFile.createWriteStream();
        writeStream.on("finish", () => resolve(googleFile.cloudStorageURI.href));
        writeStream.on("error", error => reject(error));

        const response = await axios.get(url, { responseType: "stream" });
        response.data.pipe(writeStream);
    });
}

async function getGoogleServiceCredentials(s3: S3): Promise<any> {
    try {
        const googleServiceCredentialsS3Bucket = configVariables.get("ConfigFileBucket");
        const googleServiceCredentialsS3Key = configVariables.get("ConfigFileKey");

        const data = await s3.getObject({
            Bucket: googleServiceCredentialsS3Bucket,
            Key: googleServiceCredentialsS3Key,
        }).promise();

        return JSON.parse(data.Body.toString());
    } catch (error) {
        throw new McmaException("Failed to obtain Google Service Credentials", error);
    }
}
