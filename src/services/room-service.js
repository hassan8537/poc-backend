const { handlers } = require("../utilities/handlers/handlers");
const { docClient, s3 } = require("../config/dynamodb");
const { v4: uuidv4 } = require("uuid");
const path = require("path");

class RoomService {
  constructor() {
    this.tableName = "InventoryManagement";
    this.userPK = "USER#123";
    this.elyssePocMedia = "elysse-poc-media";
  }

  async validateProject(projectId, res) {
    if (!projectId) {
      handlers.logger.failed({ message: "Project ID is required" });
      handlers.response.failed({ res, message: "Project ID is required" });
      return false;
    }

    const projectKey = {
      PK: this.userPK,
      SK: `PROJECT#${projectId}`
    };

    const project = await docClient
      .get({ TableName: this.tableName, Key: projectKey })
      .promise();

    if (!project.Item) {
      handlers.logger.failed({ message: "Project ID is required" });
      handlers.response.failed({ res, message: "Invalid project ID" });
      return false;
    }

    return true;
  }

  async validateRoom(projectId, roomId, res) {
    if (!roomId) {
      handlers.logger.failed({ message: "Room ID is required" });
      handlers.response.failed({ res, message: "Room ID is required" });
      return false;
    }

    const roomKey = {
      PK: this.userPK,
      SK: `PROJECT#${projectId}#ROOM#${roomId}`
    };

    const room = await docClient
      .get({ TableName: this.tableName, Key: roomKey })
      .promise();

    if (!room.Item) {
      handlers.logger.failed({ message: "Invalid room ID" });
      handlers.response.failed({ res, message: "Invalid room ID" });
      return false;
    }

    return true;
  }

  // async uploadVideoToS3(req, res) {
  //   const file = req.files?.videos?.[0];

  //   handlers.logger.success({
  //     message: "Received upload request",
  //     hasFile: !!file
  //   });

  //   if (!file) {
  //     handlers.logger.failed({ message: "No video file provided" });
  //     return handlers.response.failed({
  //       res,
  //       message: "No video file provided"
  //     });
  //   }

  //   const jobId = uuidv4();
  //   const originalFileName = path.basename(file.originalname || "video.mp4");
  //   const uniqueFileKey = `input/${jobId}-${originalFileName.replace(/\s+/g, "_")}`;

  //   handlers.logger.success({
  //     message: "Preparing upload to S3",
  //     data: {
  //       jobId,
  //       originalFileName,
  //       uniqueFileKey,
  //       mimeType: file.mimetype,
  //       bucket: this.elyssePocMedia
  //     }
  //   });

  //   const params = {
  //     Bucket: this.elyssePocMedia,
  //     Key: uniqueFileKey,
  //     Body: file.buffer, // <-- No file system access, use buffer
  //     ContentType: file.mimetype,
  //     ACL: "public-read"
  //   };

  //   try {
  //     handlers.logger.success({ message: "Starting upload to S3..." });

  //     const uploadResult = await s3.upload(params).promise();

  //     handlers.logger.success({
  //       message: "Upload completed",
  //       data: {
  //         location: uploadResult.Location,
  //         key: uniqueFileKey
  //       }
  //     });

  //     return handlers.response.success({
  //       res,
  //       message: "Video uploaded successfully",
  //       data: {
  //         url: uploadResult.Location,
  //         jobId,
  //         key: uniqueFileKey
  //       }
  //     });
  //   } catch (err) {
  //     console.error({ err });

  //     handlers.logger.error({
  //       message: "Failed to upload video to S3",
  //       error: err
  //     });

  //     return handlers.response.error({
  //       res,
  //       message: "Failed to upload video to S3"
  //     });
  //   }
  // }

  async uploadVideoToS3(req, res) {
    const file = req.files?.videos?.[0];

    handlers.logger.success({
      message: "Received upload request",
      hasFile: !!file
    });

    if (!file) {
      handlers.logger.failed({ message: "No video file provided" });
      return handlers.response.failed({
        res,
        message: "No video file provided"
      });
    }

    const jobId = uuidv4();
    const originalFileName = path.basename(file.originalname || "video.mp4");
    const uniqueFileKey = `input/${jobId}-${originalFileName.replace(/\s+/g, "_")}`;

    handlers.logger.success({
      message: "Preparing multipart upload to S3",
      data: {
        jobId,
        originalFileName,
        uniqueFileKey,
        mimeType: file.mimetype,
        bucket: this.elyssePocMedia
      }
    });

    const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB per part (minimum allowed by S3 except last part)
    const buffer = file.buffer;
    const totalParts = Math.ceil(buffer.length / CHUNK_SIZE);

    try {
      // 1. Create multipart upload
      const multipart = await s3
        .createMultipartUpload({
          Bucket: this.elyssePocMedia,
          Key: uniqueFileKey,
          ContentType: file.mimetype,
          ACL: "public-read"
        })
        .promise();

      const uploadId = multipart.UploadId;
      handlers.logger.success({
        message: `Multipart upload started with UploadId: ${uploadId}`
      });

      // 2. Upload parts in parallel (or sequentially)
      const uploadPartPromises = [];

      for (let partNumber = 1; partNumber <= totalParts; partNumber++) {
        const start = (partNumber - 1) * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, buffer.length);

        const partParams = {
          Bucket: this.elyssePocMedia,
          Key: uniqueFileKey,
          PartNumber: partNumber,
          UploadId: uploadId,
          Body: buffer.slice(start, end)
        };

        uploadPartPromises.push(s3.uploadPart(partParams).promise());
      }

      // Wait for all parts to upload
      const uploadedParts = await Promise.all(uploadPartPromises);

      // Prepare parts info for completing upload
      const parts = uploadedParts.map((part, index) => ({
        ETag: part.ETag,
        PartNumber: index + 1
      }));

      // 3. Complete multipart upload
      const completeParams = {
        Bucket: this.elyssePocMedia,
        Key: uniqueFileKey,
        UploadId: uploadId,
        MultipartUpload: {
          Parts: parts
        }
      };

      const completeResult = await s3
        .completeMultipartUpload(completeParams)
        .promise();

      handlers.logger.success({
        message: "Multipart upload completed",
        data: {
          location: completeResult.Location,
          key: uniqueFileKey
        }
      });

      return handlers.response.success({
        res,
        message: "Video uploaded successfully",
        data: {
          url: completeResult.Location,
          jobId,
          key: uniqueFileKey
        }
      });
    } catch (err) {
      console.error({ err });

      // Abort multipart upload if failed
      if (uploadId) {
        await s3
          .abortMultipartUpload({
            Bucket: this.elyssePocMedia,
            Key: uniqueFileKey,
            UploadId: uploadId
          })
          .promise();
        handlers.logger.error({
          message: "Aborted multipart upload due to failure"
        });
      }

      handlers.logger.error({
        message: "Failed to upload video to S3",
        error: err
      });

      return handlers.response.error({
        res,
        message: "Failed to upload video to S3"
      });
    }
  }

  async createRoom(req, res) {
    const { projectId, name, description, videoUrl, jobId, thumbnail } =
      req.body;

    handlers.logger.success({
      message: "Received request to create room",
      data: { projectId, name, videoUrl, jobId }
    });

    if (!projectId || !videoUrl || !jobId) {
      const missingField = !projectId
        ? "Project ID"
        : !videoUrl
          ? "Video URL"
          : "Job ID";

      handlers.logger.failed({
        message: `${missingField} is required`
      });

      return handlers.response.failed({
        res,
        message: `${missingField} is required`
      });
    }

    if (!(await this.validateProject(projectId, res))) {
      handlers.logger.failed({
        message: `Project validation failed for projectId: ${projectId}`
      });
      return;
    }

    try {
      const roomId = uuidv4();
      const createdAt = new Date().toISOString();

      const roomItem = {
        PK: this.userPK,
        SK: `PROJECT#${projectId}#ROOM#${roomId}`,
        EntityType: "Room",
        ProjectId: projectId,
        RoomId: roomId,
        Name: name,
        Image:
          "https://images.unsplash.com/photo-1692803629992-90acbafd964d?q=80&w=2736&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D",
        Description: description,
        Video: videoUrl,
        Thumbnail: thumbnail,
        JobId: jobId,
        CreatedAt: createdAt
      };

      handlers.logger.success({
        message: "Saving room item to DynamoDB",
        data: { roomId, table: this.tableName }
      });

      await docClient
        .put({
          TableName: this.tableName,
          Item: roomItem
        })
        .promise();

      handlers.logger.success({
        message: "Room created successfully",
        data: roomItem
      });

      return handlers.response.success({
        res,
        message: "Room created successfully",
        data: roomItem
      });
    } catch (error) {
      handlers.logger.error({
        message: "Error occurred while creating room",
        error: error.message || error
      });

      return handlers.response.error({
        res,
        message: "Failed to create room"
      });
    }
  }

  async getRoom(req, res) {
    const { projectId, roomId } = req.params;

    if (!projectId || !roomId) {
      const message = !projectId
        ? "Project ID is required"
        : "Room ID is required";
      handlers.logger.failed({ message });
      return handlers.response.failed({ res, message });
    }

    if (!(await this.validateProject(projectId, res))) return;
    if (!(await this.validateRoom(projectId, roomId, res))) return;

    try {
      const result = await docClient
        .get({
          TableName: this.tableName,
          Key: {
            PK: this.userPK,
            SK: `PROJECT#${projectId}#ROOM#${roomId}`
          }
        })
        .promise();

      if (!result.Item) {
        handlers.logger.failed({ message: "Invalid room ID" });
        return handlers.response.failed({ res, message: "Invalid room ID" });
      }

      const room = result.Item;

      // If no JobId, return room as is
      if (!room.JobId) {
        handlers.logger.success({
          message: "Room fetched successfully (no JobId)",
          data: room
        });
        return handlers.response.success({
          res,
          message: "Room fetched successfully",
          data: room
        });
      }

      // Job flow
      const jobId = room.JobId;
      const bucket = this.elyssePocMedia;
      const outputPrefix = `output/${jobId}/`;

      const getFileContent = async (key) => {
        try {
          const data = await s3
            .getObject({ Bucket: bucket, Key: `${outputPrefix}${key}` })
            .promise();
          return data.Body.toString("utf-8").trim();
        } catch {
          return null;
        }
      };

      const [errorText, resultText] = await Promise.all([
        getFileContent("error.txt"),
        getFileContent("result.txt")
      ]);

      let Accessories = null;

      if (errorText) {
        try {
          console.log({ errorText });

          Accessories = JSON.parse(errorText);
        } catch {
          Accessories = { error: errorText };
        }
      } else if (resultText) {
        try {
          console.log({ resultText });

          Accessories = JSON.parse(resultText);
        } catch {
          Accessories = { result: resultText };
        }
      }

      const responsePayload = {
        ...room,
        Accessories: room.Accessories || Accessories
      };

      handlers.logger.success({
        message: "Room fetched successfully with job output",
        data: responsePayload
      });

      return handlers.response.success({
        res,
        message: "Room fetched successfully",
        data: responsePayload
      });
    } catch (error) {
      handlers.logger.error({ message: error });
      return handlers.response.error({
        res,
        message: "Failed to fetch room"
      });
    }
  }

  async getRooms(req, res) {
    const { projectId } = req.params;

    if (!projectId) {
      handlers.logger.error({ message: "Project ID is required" });
      return handlers.response.error({
        res,
        message: "Project ID is required"
      });
    }

    if (!(await this.validateProject(projectId, res))) return;

    let fetchedItems = [];
    let nextKey = undefined;

    try {
      do {
        const result = await docClient
          .query({
            TableName: this.tableName,
            KeyConditionExpression: "PK = :pk AND begins_with(SK, :skPrefix)",
            FilterExpression: "EntityType = :entityType",
            ExpressionAttributeValues: {
              ":pk": this.userPK,
              ":skPrefix": `PROJECT#${projectId}#ROOM#`,
              ":entityType": "Room"
            },
            ExclusiveStartKey: nextKey
          })
          .promise();

        fetchedItems = [...fetchedItems, ...result.Items];
        nextKey = result.LastEvaluatedKey;
      } while (nextKey);

      if (!fetchedItems.length) {
        handlers.logger.success({ message: "No rooms yet" });
        return handlers.response.success({ res, message: "No rooms yet" });
      }

      // Helper to get file content
      const getFileContent = async (bucket, outputPrefix, key) => {
        try {
          const data = await s3
            .getObject({ Bucket: bucket, Key: `${outputPrefix}${key}` })
            .promise();
          return data.Body.toString("utf-8").trim();
        } catch {
          return null;
        }
      };

      const bucket = this.elyssePocMedia;

      // For each room, attach Accessories if JobId exists
      const enrichedRooms = await Promise.all(
        fetchedItems.map(async (room) => {
          if (!room.JobId) return room;

          const outputPrefix = `output/${room.JobId}/`;

          const [errorText, resultText] = await Promise.all([
            getFileContent(bucket, outputPrefix, "error.txt"),
            getFileContent(bucket, outputPrefix, "result.txt")
          ]);

          let Accessories = null;

          if (errorText) {
            try {
              Accessories = JSON.parse(errorText);
            } catch {
              Accessories = { error: errorText };
            }
          } else if (resultText) {
            try {
              Accessories = JSON.parse(resultText);
            } catch {
              Accessories = { result: resultText };
            }
          }

          return {
            ...room,
            Accessories: room.Accessories || Accessories
          };
        })
      );

      // Sort by CreatedAt descending
      enrichedRooms.sort(
        (a, b) => new Date(b.CreatedAt) - new Date(a.CreatedAt)
      );

      handlers.logger.success({
        message: "Rooms fetched successfully",
        data: enrichedRooms
      });
      return handlers.response.success({
        res,
        message: "Rooms fetched successfully",
        data: enrichedRooms
      });
    } catch (error) {
      handlers.logger.error({ message: error });
      return handlers.response.error({
        res,
        message: "Failed to fetch rooms"
      });
    }
  }

  async updateRoom(req, res) {
    const { projectId, roomId } = req.params;
    const { name, description, accessories, videoUrl } = req.body;

    if (!projectId || !roomId) {
      handlers.logger.error({
        message: !projectId ? "Project ID is required" : "Room ID is required"
      });
      return handlers.response.error({
        res,
        message: !projectId ? "Project ID is required" : "Room ID is required"
      });
    }

    if (!(await this.validateProject(projectId, res))) return;

    if (!(await this.validateRoom(projectId, roomId, res))) return;

    const updates = [];
    const exprValues = {};
    const exprNames = {};

    if (name) {
      updates.push("#N = :name");
      exprNames["#N"] = "Name";
      exprValues[":name"] = name;
    }
    if (description) {
      updates.push("Description = :desc");
      exprValues[":desc"] = description;
    }
    if (videoUrl) {
      updates.push("Video = :video");
      exprValues[":video"] = videoUrl;
    }
    if (accessories) {
      updates.push("Accessories = :acc");
      exprValues[":acc"] = accessories;
    }

    if (updates.length === 0) {
      handlers.logger.failed({
        message: "No updates provided"
      });
      return handlers.response.failed({
        res,
        message: "No updates provided"
      });
    }

    try {
      const result = await docClient
        .update({
          TableName: this.tableName,
          Key: {
            PK: this.userPK,
            SK: `PROJECT#${projectId}#ROOM#${roomId}`
          },
          UpdateExpression: `SET ${updates.join(", ")}`,
          ExpressionAttributeValues: exprValues,
          ExpressionAttributeNames: Object.keys(exprNames).length
            ? exprNames
            : undefined,
          ReturnValues: "ALL_NEW"
        })
        .promise();

      handlers.logger.success({
        message: "Room updated successfully",
        data: result.Attributes
      });
      return handlers.response.success({
        res,
        message: "Room updated successfully",
        data: result.Attributes
      });
    } catch (error) {
      handlers.logger.error({ message: error });
      return handlers.response.error({ res, message: "Failed to update room" });
    }
  }

  async deleteRoom(req, res) {
    const { projectId, roomId } = req.params;

    if (!projectId || !roomId) {
      handlers.logger.error({
        message: !projectId ? "Project ID is required" : "Room ID is required"
      });
      return handlers.response.error({
        res,
        message: !projectId ? "Project ID is required" : "Room ID is required"
      });
    }

    if (!(await this.validateProject(projectId, res))) return;

    if (!(await this.validateRoom(projectId, roomId, res))) return;

    try {
      await docClient
        .delete({
          TableName: this.tableName,
          Key: {
            PK: this.userPK,
            SK: `PROJECT#${projectId}#ROOM#${roomId}`
          }
        })
        .promise();

      handlers.logger.success({
        message: "Room deleted successfully"
      });
      return handlers.response.success({
        res,
        message: "Room deleted successfully"
      });
    } catch (error) {
      handlers.logger.error({ message: error });
      return handlers.response.error({ res, message: "Failed to delete room" });
    }
  }
}

module.exports = new RoomService();
