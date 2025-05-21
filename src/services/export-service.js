const ExcelJS = require("exceljs");
const nodemailer = require("nodemailer");
const { handlers } = require("../utilities/handlers/handlers");
const { docClient, s3 } = require("../config/dynamodb");

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
      handlers.logger.failed({ message: "Project ID is required" });
      handlers.response.failed({ res, message: "Invalid project ID" });
      return false;
    }

    return true;
  }

  async generateAndEmailExcel({ enrichedRooms, email }) {
    try {
      handlers.logger.success({
        message: `Generating Excel for ${enrichedRooms.length} rooms`
      });

      enrichedRooms.sort(
        (a, b) => new Date(b.CreatedAt) - new Date(a.CreatedAt)
      );

      const workbook = new ExcelJS.Workbook();

      enrichedRooms.forEach((room) => {
        const sheetName = `Room ${room.RoomId}` || "Unnamed Room";
        const sheet = workbook.addWorksheet(sheetName);

        sheet.columns = [
          { header: "Project ID", key: "ProjectId", width: 20 },
          { header: "Room ID", key: "RoomId", width: 30 },
          { header: "Name", key: "Name", width: 30 },
          { header: "Description", key: "Description", width: 40 },
          { header: "Video URL", key: "Video", width: 50 },
          { header: "Thumbnail", key: "Thumbnail", width: 50 },
          { header: "Job ID", key: "JobId", width: 30 },
          { header: "Created At", key: "CreatedAt", width: 30 },
          { header: "Accessories", key: "Accessories", width: 50 }
        ];

        sheet.addRow({
          ...room,
          Accessories: room.Accessories ? JSON.stringify(room.Accessories) : ""
        });
      });

      const buffer = await workbook.xlsx.writeBuffer();

      handlers.logger.success({
        message: "Excel file generated, preparing to send email"
      });

      const transporter = nodemailer.createTransport({
        host: "email-smtp.us-east-1.amazonaws.com",
        port: 587,
        secure: false,
        auth: {
          user: "AKIAWVYDXY45EALOR4FQ",
          pass: "BKjPE3kujw6mkCHKFsTCxtTM0xNQbNzk4YF19L2uW6fC"
        }
      });

      await transporter.sendMail({
        from: "Elysse@cluedotech.com",
        to: email,
        subject: "Room Data Export",
        text: "Attached is the Excel file containing room data.",
        attachments: [
          {
            filename: "room-data.xlsx",
            content: buffer
          }
        ]
      });

      handlers.logger.success({
        message: `Email sent successfully to ${email}`
      });
    } catch (error) {
      handlers.logger.error({
        message: "Error in generateAndEmailExcel",
        error
      });
      throw error; // Propagate error to caller for proper handling
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
      let nextKey = undefined;

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
        handlers.logger.success({ message: "No rooms found" });
        return handlers.response.success({ res, message: "No rooms yet" });
      }

      const bucket = this.elyssePocMedia;

      const getFileContent = async (bucket, outputPrefix, key) => {
        try {
          const data = await s3
            .getObject({ Bucket: bucket, Key: `${outputPrefix}${key}` })
            .promise();
          return data.Body.toString("utf-8").trim();
        } catch (err) {
          handlers.logger.success({
            message: `Failed to get file content: ${outputPrefix}${key}`,
            error: err
          });
          return null;
        }
      };

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

      await this.generateAndEmailExcel({ enrichedRooms, email });

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
        message: "Failed to export rooms"
      });
    }
  }
}

module.exports = new Service();
