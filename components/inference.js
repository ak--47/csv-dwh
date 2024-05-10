const { isJSONStr } = require('ak-tools');
const { parseISO, isValid } = require('date-fns');

function inferType(value) {
	//complex objects
	if (isBoolean(value)) return 'BOOLEAN';
	if (isJSONStr(value)) return inferJSONType(value);
	if (isNumber(value)) return inferNumberType(value);
	if (isValidDate(value)) return inferDateType(value);

	return 'STRING';
}

function isBoolean(value) {
	const val = value.toLowerCase();
	return val === 'true' || val === 'false';
}

function isNumber(value) {
	return !isNaN(value) && !isNaN(parseFloat(value));
}

function inferNumberType(value) {
	return value.includes('.') ? 'FLOAT' : 'INT';
}

function inferDateType(value) {

	if (value.includes('T')) {
		return 'TIMESTAMP';
	}
	if (value.includes('Z')) {
		return 'TIMESTAMP';
	}

	return 'DATE';
}

function inferJSONType(value) {
	if (value.startsWith('[') && value.endsWith(']')) {
		return 'ARRAY';
	}
	if (value.startsWith('{') && value.endsWith('}')) {
		return 'OBJECT';
	};
	return 'JSON';
}

function range(a, b, step = 1) {
	step = !step ? 1 : step;
	b = b / step;
	for (var i = a; i <= b; i++) {
		this.push(i * step);
	}
	return this;
};


function isValidDate(value) {
	return isValid(parseISO(value));
}


function getUniqueKeys(data) {
	const keysSet = new Set();
	data.forEach(item => {
		Object.keys(item).forEach(key => keysSet.add(key));
	});
	return Array.from(keysSet);
}


function generateSchema(data, type) {
	const keys = getUniqueKeys(data);
	//assume everything is a string
	let schema = keys.map(key => {
		const template = {
			name: key,
			type: 'STRING'
		};

		try {
			while (!data[0][key] && data.length) {
				data.shift();
			}
			template.type = inferType(data[0][key]);

		}
		catch (e) {
			console.error(e);
		}
		return template;
	});
	return schema;
}

/**
 * Prepares and cleans header names according to BigQuery's naming restrictions. can return hashmap or array
 * ? see: https://cloud.google.com/bigquery/docs/schemas#column_names
 * @param {string[]} headers - The array of header names to be cleaned.
 * @param {boolean} [asArray=false] - Whether to return the result as an array of arrays.
 * @returns {(Object|string[][])|{}} If asArray is true, returns an array of arrays, each containing
 * the original header name and the cleaned header name. If false, returns an object mapping
 * original header names to cleaned header names.
 */
function prepHeaders(headers, asArray = false) {
	const headerMap = {};
	const usedNames = new Set();

	headers.forEach(originalName => {
		let cleanName = originalName.trim();

		// Replace invalid characters
		cleanName = cleanName.replace(/[^a-zA-Z0-9_]/g, '_');

		// Ensure it starts with a letter or underscore
		if (!/^[a-zA-Z_]/.test(cleanName)) {
			cleanName = '_' + cleanName;
		}

		// Trim to maximum length
		cleanName = cleanName.substring(0, 300);

		// Ensure uniqueness
		let uniqueName = cleanName;
		let suffix = 1;
		while (usedNames.has(uniqueName)) {
			uniqueName = cleanName + '_' + suffix++;
		}
		cleanName = uniqueName;

		// Add to used names set
		usedNames.add(cleanName);

		// Map original name to clean name
		headerMap[originalName] = cleanName;
	});

	if (asArray) {
		const oldNames = Object.keys(headerMap);
		const newNames = Object.values(headerMap);
		const result = oldNames.map((key, i) => [key, newNames[i]]);
		return result;
	}

	return headerMap;
}

module.exports = {
	inferType,
	range,
	getUniqueKeys,
	inferJSONType,
	inferNumberType,
	isNumber,
	isBoolean,
	isJSONStr,
	generateSchema,
	prepHeaders
};