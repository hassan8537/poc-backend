const multer = require("multer");
const path = require("path");

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    if (file.mimetype.startsWith("image")) {
      cb(null, "uploads/images");
    } else if (file.mimetype.startsWith("video")) {
      cb(null, "uploads/videos");
    } else if (file.mimetype.startsWith("audio")) {
      cb(null, "uploads/audios");
    } else {
      cb(null, "uploads/docs");
    }
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + path.extname(file.originalname));
  }
});

const upload = multer({ storage: multer.memoryStorage() });

module.exports = upload;
