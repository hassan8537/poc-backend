const bcrypt = require("bcryptjs");

const comparePassword = async ({ plainPassword, hashedPassword }) => {
  try {
    const isMatch = await bcrypt.compare(plainPassword, hashedPassword);
    return isMatch;
  } catch (error) {
    handlers.logger.error({ message: error });
    return handlers.response.error({ res, message: error.message });
  }
};

module.exports = comparePassword;
