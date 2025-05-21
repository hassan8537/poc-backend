// lambda.js
const serverlessExpress = require("@vendia/serverless-express");
const app = require("./server"); // This should export the Express app

exports.handler = serverlessExpress({ app });
