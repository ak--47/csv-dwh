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


module.exports = {
	inferType,
	range,
	getUniqueKeys,
	inferJSONType,
	inferNumberType,
	isNumber,
	isBoolean,
	isJSONStr,
	generateSchema
};