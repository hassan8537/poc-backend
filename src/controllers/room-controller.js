class Controller {
  constructor() {
    this.service = require("../services/room-service");
  }

  async getPresignedUploadUrl(req, res) {
    await this.service.getPresignedUploadUrl(req, res);
  }

  async createRoom(req, res) {
    await this.service.createRoom(req, res);
  }

  async getRoom(req, res) {
    await this.service.getRoom(req, res);
  }

  async getRooms(req, res) {
    await this.service.getRooms(req, res);
  }

  async updateRoom(req, res) {
    await this.service.updateRoom(req, res);
  }

  async deleteRoom(req, res) {
    await this.service.deleteRoom(req, res);
  }
}

module.exports = new Controller();
