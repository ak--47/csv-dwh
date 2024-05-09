// @ts-nocheck
const { generateSchema, getUniqueKeys, inferType, } = require('../components/inference.js');

describe('INFER', () => {
    test('STRING', () => {
        expect(inferType("hello")).toBe('STRING');
        expect(inferType("")).toBe('STRING'); // Empty string
        expect(inferType("2021-04-32")).toBe('STRING'); // Invalid date
        expect(inferType("true love")).toBe('STRING'); // Contains boolean but is string
    });

    test('DATE', () => {
        expect(inferType("2021-04-01")).toBe('DATE');
        expect(inferType("2021-02-28T01:02:03Z")).toBe('TIMESTAMP'); // ISO format
        expect(inferType("2021-04-01T00:00:00")).toBe('TIMESTAMP'); // Date with time
    });

    test('BOOLEAN', () => {
        expect(inferType("true")).toBe('BOOLEAN');
        expect(inferType("false")).toBe('BOOLEAN');
        expect(inferType("TRUE")).toBe('BOOLEAN'); // Case insensitivity
        expect(inferType("FALSE")).toBe('BOOLEAN');
        expect(inferType("TrUe")).toBe('BOOLEAN');
    });

    test('FLOAT', () => {
        expect(inferType("123.456")).toBe('FLOAT');
        expect(inferType(".456")).toBe('FLOAT'); // Leading dot
        expect(inferType("123.")).toBe('FLOAT'); // Trailing dot
        expect(inferType("0.123456789")).toBe('FLOAT'); // Small float
    });

    test('INT', () => {
        expect(inferType("123")).toBe('INT');
        expect(inferType("0")).toBe('INT'); // Zero
        expect(inferType("-123")).toBe('INT'); // Negative integer
        expect(inferType("+123")).toBe('INT'); // Positive sign
    });

    test('ARRAY', () => {
        expect(inferType("[1,2,3]")).toBe('ARRAY');
        expect(inferType("[]")).toBe('ARRAY'); // Empty array
        expect(inferType("[\"a\", \"b\"]")).toBe('ARRAY'); // Array of strings
        expect(inferType("[{\"a\": 1}, {\"b\": 2}]")).toBe('ARRAY'); // Array of objects
    });

    test('OBJECT', () => {
        expect(inferType("{\"key\": \"value\"}")).toBe('OBJECT');
        expect(inferType("{}")).toBe('OBJECT'); // Empty object
        expect(inferType("{\"a\": 1, \"b\": [2, 3]}")).toBe('OBJECT'); // Nested structures
    });
});


describe("GENERATE", () => {
    test('correct schema', () => {
        const data = [{ name: "Alice", age: "30", active: "true" }];
        const schema = generateSchema(data);
        expect(schema).toEqual([
            { name: 'name', type: 'STRING' },
            { name: 'age', type: 'INT' },
            { name: 'active', type: 'BOOLEAN' }
        ]);
    });

    test('mixed types', () => {
        const data = [
            { name: "Alice", age: "30", active: "false" },
            { name: "Bob", age: "thirty", active: "true" }
        ];
        const schema = generateSchema(data);
        expect(schema).toEqual([
            { name: 'name', type: 'STRING' },
            { name: 'age', type: 'INT' },  // "thirty" should forces age to be STRING, but we use first seen
            { name: 'active', type: 'BOOLEAN' }
        ]);
    });

    test('unique keys', () => {
        const data = [{ a: 1, b: 2 }, { a: 3, c: 4, d: null }];
        const uniqueKeys = getUniqueKeys(data);
        expect(uniqueKeys).toEqual(expect.arrayContaining(['a', 'b', 'c', 'd']));
        expect(uniqueKeys.length).toBe(4);
    });
});
