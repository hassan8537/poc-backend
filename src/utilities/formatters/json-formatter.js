/**
 * Safely parses JSON strings with multiple fallback strategies for malformed input.
 * Handles common JavaScript object notation issues like unquoted keys, single quotes,
 * trailing commas, comments, and undefined values.
 * 
 * @param {string} text - The string to parse as JSON
 * @returns {Object|Array|null} Parsed JSON object/array or null if parsing fails
 * 
 * @example
 * // Standard JSON
 * safeParseJSON('{"name": "John", "age": 30}') // { name: "John", age: 30 }
 * 
 * // JavaScript objcet notaton
 * safeParseJSON('{name: "John", age: 30,}') // { name: "John", age: 30 }
 * 
 * // With comments and single quots
 * safeParseJSON("{'name': 'John', // comment\n 'age': 30}") // { name: "John", age: 30 }
 */
const safeParseJSON = (text) => {
  if (!text || typeof text !== 'string') {
    return null;
  }
  
  const trimmed = text.trim();
  if (!trimmed) {
    return null;
  }

  try {
    return JSON.parse(trimmed);
  } catch {
    try {
      let fixed = trimmed
        .replace(/([{,]\s*)([a-zA-Z_$][a-zA-Z0-9_$\-\.]*)\s*:/g, '$1"$2":')
        .replace(/:\s*'([^']*)'/g, ':"$1"')
        .replace(/'([^']*)':/g, '"$1":')
        .replace(/,(\s*[}\]])/g, '$1')
        .replace(/\/\*[\s\S]*?\*\//g, '')
        .replace(/\/\/.*$/gm, '')
        .replace(/:\s*undefined\b/g, ': null')
        .replace(/:\s*"(true|false|null)"/g, ': $1')
        .replace(/:\s*"(\d+\.?\d*)"/g, ': $1');

      return JSON.parse(fixed);
    } catch {
      try {
        let aggressive = trimmed
          .replace(/[\x00-\x1F\x7F]/g, '')
          .replace(/^[^{\[]*/, '')
          .replace(/[^}\]]*$/, '')
          .replace(/,+/g, ',')
          .replace(/}\s*{/g, '},{')
          .replace(/]\s*\[/g, '],[')
          .replace(/([{,]\s*)([a-zA-Z_$][a-zA-Z0-9_$\-\.]*)\s*:/g, '$1"$2":')
          .replace(/:\s*'([^']*)'/g, ':"$1"')
          .replace(/,(\s*[}\]])/g, '$1')
          .replace(/:\s*undefined\b/g, ': null');

        return JSON.parse(aggressive);
      } catch {
        try {
          const jsonMatch = trimmed.match(/[{\[].*[}\]]/s);
          if (jsonMatch) {
            return JSON.parse(jsonMatch[0]);
          }
        } catch {
          return null;
        }
      }
    }
  }

  return null;
};

module.exports = safeParseJSON;