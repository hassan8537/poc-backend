const fs = require("fs");
const path = require("path");
const { v4: uuidv4 } = require("uuid");
const ffmpeg = require("fluent-ffmpeg");

ffmpeg.setFfmpegPath("/usr/bin/ffmpeg");
ffmpeg.setFfprobePath("/usr/bin/ffprobe");

const { handlers } = require("../../utilities/handlers/handlers");
const { s3 } = require("../../config/dynamodb");

exports.generateAndUploadThumbnailFromS3Url = async function (
  videoUrl,
  bucket
) {
  if (!videoUrl) {
    handlers.logger.error({ message: "No video URL provided" });
    throw new Error("No video URL provided for thumbnail generation");
  }

  const thumbnailFilename = `${Date.now()}_thumb.jpeg`;
  const localThumbnailPath = path.join(
    "uploads",
    "thumbnails",
    thumbnailFilename
  );

  fs.mkdirSync(path.dirname(localThumbnailPath), { recursive: true });

  try {
    handlers.logger.info({
      message: `Starting thumbnail generation from URL: ${videoUrl}`
    });

    // Step 1: Generate thumbnail
    await new Promise((resolve, reject) => {
      ffmpeg(videoUrl)
        .on("start", (commandLine) => {
          handlers.logger.info({ message: `FFmpeg command: ${commandLine}` });
        })
        .on("stderr", (stderrLine) => {
          handlers.logger.info({ message: `FFmpeg stderr: ${stderrLine}` });
        })
        .on("end", () => {
          handlers.logger.info({
            message: `Thumbnail generated at: ${localThumbnailPath}`
          });
          resolve();
        })
        .on("error", (err) => {
          console.log({ err });
          handlers.logger.error({ message: `FFmpeg error: ${err.message}` });
          reject(err);
        })
        .screenshots({
          count: 1,
          folder: path.dirname(localThumbnailPath),
          filename: thumbnailFilename,
          size: "320x240"
        });
    });

    // Step 2: Upload thumbnail to S3
    const fileKey = `${uuidv4()}.jpeg`;
    const uploadParams = {
      Bucket: bucket,
      Key: fileKey,
      Body: fs.createReadStream(localThumbnailPath),
      ContentType: "image/jpeg",
      ACL: "public-read"
    };

    handlers.logger.info({ message: `Uploading thumbnail to S3: ${fileKey}` });
    const uploadResult = await s3.upload(uploadParams).promise();

    fs.unlink(localThumbnailPath, (err) => {
      if (err) {
        handlers.logger.warn({
          message: `Failed to delete local thumbnail: ${err.message}`
        });
      }
    });

    handlers.logger.success({
      message: "Thumbnail uploaded successfully",
      data: { url: uploadResult.Location }
    });

    return uploadResult.Location;
  } catch (error) {
    fs.unlink(localThumbnailPath, () => {});
    handlers.logger.error({
      message: `Thumbnail generation/upload failed: ${error.message}`
    });
    throw new Error("Failed to generate or upload thumbnail");
  }
};
