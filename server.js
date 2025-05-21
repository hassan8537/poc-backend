require("dotenv").config();

global.rootDir = __dirname;

const express = require("express");
const session = require("express-session");
const bodyParser = require("body-parser");
const cookieParser = require("cookie-parser");
const cors = require("cors");
const morgan = require("morgan");
const { handlers } = require("./src/utilities/handlers/handlers");
const path = require("path");

const port = process.env.PORT || 3000;
const nodeEnv = process.env.NODE_ENV || "development";
const secretKey = process.env.SECRET_KEY;
const maxAge = Number(process.env.MAX_AGE) || 2592000000;
const baseUrl = process.env.BASE_URL;

const app = express();

app.use("/uploads", express.static(path.join(__dirname, "uploads")));

const server = require("http").createServer(app);

app.use(
  session({
    secret: secretKey,
    resave: false,
    saveUninitialized: false,
    cookie: { maxAge }
  })
);

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(cookieParser());
app.use(cors());
app.use(morgan("tiny"));

app.get("/", (req, res) => res.send("Hello World"));

const userRoutes = require("./src/routes/index");
app.use(userRoutes);

server.listen(port, () => {
  handlers.logger.success({ message: `POC is live at ${baseUrl}` });
});

module.exports = app;
