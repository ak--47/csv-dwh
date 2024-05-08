// @ts-nocheck
const { generateSchema, getUniqueKeys, inferType, } = require('../components/inference.js');

describe('INFER', () => {
	test('STRING', () => {
		expect(inferType("hello")).toBe('STRING');
	});

	test('DATE', () => {
		expect(inferType("2021-04-01")).toBe('DATE');
	});

	test('BOOLEAN', () => {
		expect(inferType("true")).toBe('BOOLEAN');
		expect(inferType("false")).toBe('BOOLEAN');
	});

	test('FLOAT', () => {
		expect(inferType("123.456")).toBe('FLOAT');
	});

	test('INT', () => {
		expect(inferType("123")).toBe('INT');
	});

	test('ARRAY', () => {
		expect(inferType("[1,2,3]")).toBe('ARRAY');
	});

	test('OBJECT', () => {
		expect(inferType("{\"key\": \"value\"}")).toBe('OBJECT');
	});

});

describe("GENERATE", () => {
	test('correct schema', () => {
		const data = [{ name: "Alice", age: "30" }];
		const schema = generateSchema(data);
		expect(schema).toEqual([
			{ name: 'name', type: 'STRING' },
			{ name: 'age', type: 'INT' }
		]);
	});

	test('unique keys', () => {
		const data = [{ a: 1, b: 2 }, { a: 3, c: 4 }];
		expect(getUniqueKeys(data)).toEqual(expect.arrayContaining(['a', 'b', 'c']));
	});
});
