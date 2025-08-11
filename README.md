# AWS Serverless POC Backend

A comprehensive Node.js backend application demonstrating AWS serverless architecture patterns, featuring DynamoDB, S3, Lambda, and SES integrations for inventory management and media processing.

## 🏗️ Architecture Overview

This application showcases a modern serverless architecture built on AWS services:

- **AWS Lambda**: Serverless compute for request handling
- **Amazon DynamoDB**: NoSQL database for inventory and project data
- **Amazon S3**: Object storage for media files and generated content
- **Amazon SES**: Email service for notifications and exports
- **Docker**: Containerized deployment ready for AWS Lambda

## 🚀 Key Features

### 📊 Inventory Management System
- **Project Management**: Create, read, update, and delete projects with hierarchical data structure
- **Room Management**: Organize inventory items by rooms within projects
- **Media Processing**: Upload, process, and generate thumbnails for images and videos
- **Data Export**: Generate Excel reports and email them to users

### 🔧 AWS Service Integrations

#### DynamoDB Integration
- Single-table design with composite keys (PK/SK pattern)
- Hierarchical data structure: `USER#123` → `PROJECT#id` → `ROOM#id`
- Efficient querying with partition and sort keys
- Document client for simplified operations

#### S3 Integration
- **Media Storage**: Upload images and videos to S3 buckets
- **Presigned URLs**: Secure, temporary access for file uploads
- **Thumbnail Generation**: Automated thumbnail creation using FFmpeg
- **Content Retrieval**: Stream media files for processing

#### Lambda Deployment
- **Serverless Express**: Uses `@vendia/serverless-express` for Express.js compatibility
- **Docker Support**: AWS Lambda base image with Node.js 18.x
- **Handler Configuration**: Optimized for AWS Lambda runtime environment

#### SES Email Service
- **SMTP Integration**: Configured for `email-smtp.us-east-1.amazonaws.com`
- **Excel Export Email**: Automated email delivery of generated reports
- **Transactional Emails**: System notifications and alerts

## 🛠️ Technology Stack

### Core Technologies
- **Node.js 18.x**: Runtime environment
- **Express.js**: Web application framework
- **AWS SDK v2**: AWS service integrations

### Key Dependencies
- `aws-sdk`: AWS service client library
- `aws-cdk-lib`: AWS Cloud Development Kit for infrastructure
- `@vendia/serverless-express`: Lambda-Express adapter
- `mongoose`: MongoDB ODM (hybrid database approach)
- `multer`: File upload handling
- `exceljs`: Excel file generation
- `nodemailer`: Email service integration
- `fluent-ffmpeg`: Video/audio processing

## 📁 Project Structure

```
├── lambda.js                 # AWS Lambda entry point
├── server.js                 # Express.js application setup
├── Dockerfile                # AWS Lambda container configuration
├── src/
│   ├── config/
│   │   ├── dynamodb.js       # AWS SDK configuration
│   │   ├── mongodb.js        # MongoDB connection
│   │   └── nodemailer.js     # Email configuration
│   ├── controllers/          # Request handlers
│   ├── services/             # Business logic with AWS integrations
│   │   ├── project-service.js    # DynamoDB project operations
│   │   ├── room-service.js       # S3 & DynamoDB room operations  
│   │   └── export-service.js     # Excel generation & SES email
│   ├── utilities/
│   │   ├── generators/
│   │   │   └── thumbnail-generator.js  # S3 thumbnail processing
│   │   └── handlers/         # Response and logging utilities
│   └── routes/               # API route definitions
```

## ⚙️ Configuration

### Environment Variables

```bash
# AWS Configuration
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key

# Application Configuration
PORT=3000
NODE_ENV=development
SECRET_KEY=your_session_secret
MAX_AGE=2592000000
BASE_URL=http://localhost:3000

# Database Configuration
MONGODB_URI=your_mongodb_connection_string

# Email Configuration (SES)
EMAIL_HOST=email-smtp.us-east-1.amazonaws.com
EMAIL_PORT=587
EMAIL_USER=your_smtp_username
EMAIL_PASSWORD=your_smtp_password
```

### AWS Resources Required

1. **DynamoDB Table**: `InventoryManagement`
   - Partition Key: `PK` (String)
   - Sort Key: `SK` (String)

2. **S3 Bucket**: `elysse-poc-media`
   - Configure for public read access for media files
   - Enable versioning for data protection

3. **SES Configuration**
   - Verify sender email addresses
   - Configure SMTP credentials

## 🚀 Deployment Options

### 1. AWS Lambda Deployment

Build and deploy the Docker container:

```bash
# Build Docker image
docker build -t aws-serverless-poc .

# Tag for ECR (replace with your ECR URI)
docker tag aws-serverless-poc:latest 123456789012.dkr.ecr.us-east-1.amazonaws.com/aws-serverless-poc:latest

# Push to ECR
docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/aws-serverless-poc:latest
```

### 2. Local Development

```bash
# Install dependencies
npm install

# Create required directories
mkdir -p uploads/{audios,videos,images,docs}

# Create .env file with required environment variables
# (see Configuration section above)

# Start development server
npm start

# Server runs on http://localhost:3000
```

## 📚 API Endpoints

### Projects
- `GET /projects` - List all projects
- `POST /projects` - Create new project
- `GET /projects/:id` - Get project details
- `PUT /projects/:id` - Update project
- `DELETE /projects/:id` - Delete project

### Rooms
- `GET /projects/:projectId/rooms` - List rooms in project
- `POST /projects/:projectId/rooms` - Create new room
- `GET /projects/:projectId/rooms/:roomId` - Get room details
- `PUT /projects/:projectId/rooms/:roomId` - Update room
- `DELETE /projects/:projectId/rooms/:roomId` - Delete room

### Media & Export
- `POST /upload` - Upload media files to S3
- `POST /export` - Generate and email Excel reports
- `GET /presigned-url` - Get S3 presigned URL for uploads

## 🔍 AWS Service Usage Examples

### DynamoDB Operations
```javascript
// Query projects for a user
const params = {
  TableName: 'InventoryManagement',
  KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
  ExpressionAttributeValues: {
    ':pk': 'USER#123',
    ':sk': 'PROJECT#'
  }
};
```

### S3 File Operations
```javascript
// Generate presigned URL for upload
const presignedUrl = await s3.getSignedUrlPromise('putObject', {
  Bucket: 'elysse-poc-media',
  Key: `uploads/${filename}`,
  ContentType: 'image/jpeg',
  Expires: 3600
});
```

### Thumbnail Generation
```javascript
// Process video and upload thumbnail to S3
const thumbnailBuffer = await generateThumbnail(videoUrl);
await s3.upload({
  Bucket: 'elysse-poc-media',
  Key: `thumbnails/${filename}`,
  Body: thumbnailBuffer,
  ContentType: 'image/jpeg'
}).promise();
```

## 🔐 Security Features

- **Environment-based Configuration**: Sensitive data stored in environment variables
- **AWS IAM Integration**: Proper role-based access control
- **CORS Configuration**: Cross-origin request handling
- **Session Management**: Express session with configurable expiry
- **Input Validation**: Joi schema validation for API requests

## 📊 Monitoring & Logging

- **Structured Logging**: Custom logging utilities for success/failure tracking
- **AWS CloudWatch Integration**: Automatic log aggregation in Lambda environment
- **Error Handling**: Comprehensive error responses with appropriate HTTP status codes

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## 📄 License

This project is licensed under the ISC License - see the [LICENSE](LICENSE) file for details.

## 🏷️ Tags

`aws` `lambda` `dynamodb` `s3` `ses` `serverless` `nodejs` `express` `docker` `inventory-management` `media-processing`
