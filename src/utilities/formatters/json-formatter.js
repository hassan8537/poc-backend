const safeParseJSON = (text) => {
  try {
    return JSON.parse(text);
  } catch {
    // Attempt to convert single quotes to double quotes and retry
    try {
      return JSON.parse(
        text
          .replace(/(['"])?([a-zA-Z0-9_]+)(['"])?:/g, '"$2":')
          .replace(/'/g, '"')
      );
    } catch {
      return null;
    }
  }
};

module.exports = safeParseJSON;
