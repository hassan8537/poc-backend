const express = require("express");
const router = express.Router();
const roomController = require("../controllers/room-controller");
const upload = require("../middlewares/multer-middleware");

router.post(
  "/rooms/uploads",
  upload.fields([{ name: "videos" }]),
  roomController.getPresignedUploadUrl.bind(roomController)
);

router.post("/rooms", roomController.createRoom.bind(roomController));

router.get(
  "/:projectId/rooms/:roomId",
  roomController.getRoom.bind(roomController)
);

router.get("/:projectId/rooms", roomController.getRooms.bind(roomController));

router.put(
  "/:projectId/rooms/:roomId",
  roomController.updateRoom.bind(roomController)
);

router.delete(
  "/:projectId/rooms/:roomId",
  roomController.deleteRoom.bind(roomController)
);

module.exports = router;
