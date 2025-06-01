const ExcelJS = require("exceljs");
const MailComposer = require("nodemailer/lib/mail-composer");
const { SESClient, SendRawEmailCommand } = require("@aws-sdk/client-ses");

const { handlers } = require("../utilities/handlers/handlers");
const { docClient, s3 } = require("../config/dynamodb");
const safeParseJSON = require("../utilities/formatters/json-formatter");

const ses = new SESClient(); // Uses Lambda's IAM role

const senderEmail = process.env.SENDER_EMAIL;

class Service {
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
      handlers.logger.failed({ message: "Invalid project ID" });
      handlers.response.failed({ res, message: "Invalid project ID" });
      return false;
    }

    return true;
  }

  async generateAndEmailExcel({ enrichedRooms, email }) {
    try {
      const receiverEmail = email?.toLowerCase()?.trim();
      const workbook = new ExcelJS.Workbook();
      const sheetName = `${enrichedRooms[0]?.projectName || "Project"} - Room Data`;
      const sheet = workbook.addWorksheet(sheetName);
      handlers.logger.success({
        message: `Generating Excel for Project Id ${enrichedRooms[0]?.ProjectId} rooms`
      });

      enrichedRooms.sort(
        (a, b) => new Date(b.CreatedAt) - new Date(a.CreatedAt)
      );

      sheet.columns = [
        { header: "Project Name", key: "projectName", width: 30 },
        { header: "Room Name", key: "Name", width: 50 },
        { header: "Item Name", key: "itemName", width: 50 },
        { header: "Quantity", key: "quantity", width: 20 }
      ];

      enrichedRooms.forEach((room) => {
        const accessories = { ...room?.Accessories };

        const accessoriesAvailable = Object.keys(accessories).length > 0;
        const accessoriesHasError = Object.keys(accessories).includes("error");

        if (accessories && accessoriesAvailable && !accessoriesHasError) {
          Object.keys(accessories).forEach((key) => {
            sheet.addRow({
              projectName: room.projectName,
              Name: room.Name,
              itemName: key,
              quantity: accessories[key]
            });
          });
        } else {
          sheet.addRow({
            projectName: room.projectName,
            Name: room.Name,
            itemName: "No items yet",
            quantity: 0
          });
        }
      });

      const buffer = await workbook.xlsx.writeBuffer();

      handlers.logger.success({
        message: "Excel file generated, preparing to send email"
      });

      if (!senderEmail) {
        throw new Error("Configure the sender email first");
      }

      if (!receiverEmail) {
        throw new Error("Recipient email is required.");
      }

      const mail = new MailComposer({
        from: senderEmail, // Must be SES verified
        to: receiverEmail,
        subject: "Room Data Export",
        text: "Attached is the Excel file containing room data.",
        attachments: [
          {
            filename: "room-data.xlsx",
            content: buffer,
            contentType:
              "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
          }
        ]
      });

      const rawMessage = await new Promise((resolve, reject) => {
        mail
          .compile()
          .build((err, message) => (err ? reject(err) : resolve(message)));
      });

      await ses.send(
        new SendRawEmailCommand({ RawMessage: { Data: rawMessage } })
      );

      handlers.logger.success({
        message: `Email sent successfully to ${email}`
      });
    } catch (error) {
      handlers.logger.error({
        message: "Error in generateAndEmailExcel",
        error
      });
      throw error;
    }
  }

  async export(req, res) {
    const { email, projectId } = req.body;

    if (!projectId) {
      handlers.logger.error({ message: "Project ID is required" });
      return handlers.response.error({
        res,
        message: "Project ID is required"
      });
    }

    try {
      if (!(await this.validateProject(projectId, res))) return;

      let fetchedItems = [];
      let nextKey;
      let projectName = "";

      do {
        // Get project details
        const projectResult = await docClient
          .query({
            TableName: this.tableName,
            KeyConditionExpression: "PK = :pk AND SK = :sk",
            FilterExpression: "EntityType = :entityType",
            ExpressionAttributeValues: {
              ":pk": this.userPK,
              ":sk": `PROJECT#${projectId}`,
              ":entityType": "Project"
            }
          })
          .promise();

        projectName = projectResult.Items?.[0]?.Name || "Unknown Project";

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
        handlers.logger.success({ message: "No rooms found" });
        return handlers.response.success({ res, message: "No rooms yet" });
      }

      const getFileContent = async (bucket, prefix, key) => {
        try {
          const data = await s3
            .getObject({ Bucket: bucket, Key: `${prefix}${key}` })
            .promise();
          return data.Body.toString("utf-8").trim();
        } catch (err) {
          handlers.logger.success({
            message: `Failed to get file content: ${prefix}${key}`,
            error: err
          });
          return null;
        }
      };

      const enrichedRooms = await Promise.all(
        fetchedItems.map(async (room) => {
          if (!room.JobId) return room;

          const roomName = room.Name?.split?.(" ")?.join?.("_");
          const outputPrefix = `output/${room.JobId}-${roomName}-room-video/`;

          const [errorText, resultText] = await Promise.all([
            getFileContent(
              this.elyssePocMedia,
              outputPrefix,
              `${room.JobId}-${roomName}-room-video-error.txt`
            ),
            getFileContent(
              this.elyssePocMedia,
              outputPrefix,
              `${room.JobId}-${roomName}-room-video-result.txt`
            )
          ]);

          let Accessories = null;

          if (errorText) {
            Accessories = safeParseJSON(errorText);
          } else if (resultText) {
            Accessories = safeParseJSON(resultText);
          }

          return {
            ...room,
            Accessories: room.Accessories || Accessories,
            projectName: projectName
          };
        })
      );

      await this.generateAndEmailExcel({
        enrichedRooms,
        email: email
      });

      handlers.logger.success({
        message: "Export and email process completed successfully"
      });
      return handlers.response.success({
        res,
        message: "Export and email sent successfully"
      });
    } catch (error) {
      handlers.logger.error({ message: error });
      return handlers.response.error({
        res,
        message: error?.message || "Failed to export rooms"
      });
    }
  }
}

module.exports = new Service();
