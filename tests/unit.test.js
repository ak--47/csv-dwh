// @ts-nocheck
const { generateSchema, getUniqueKeys, inferType, prepHeaders, cleanName } = require('../components/inference.js');

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


describe('RENAMING', () => {
	test('cleans headers', () => {
		const headers = ['first name', 'last-name', 'email@address.com', '123Start', '!@#$%^&*()'];
		const expected = {
			'first name': 'first_name',
			'last-name': 'last_name',
			'email@address.com': 'email_address_com',
			'123Start': '_123Start',
			'!@#$%^&*()': '__________'
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});

	test('unique names', () => {
		const headers = ['name', 'name', 'name'];
		const expected = {
			'name': 'name_2', //this is not ideal
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});



	test('arrays', () => {
		const headers = ['first name', 'last-name'];
		const expectedArray = [
			['first name', 'first_name'],
			['last-name', 'last_name']
		];
		expect(prepHeaders(headers, true)).toEqual(expectedArray);
	});

	test('numerics', () => {
		const headers = ['123', '4567', '890'];
		const expected = {
			'123': '_123',
			'4567': '_4567',
			'890': '_890'
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});


	test('max length', () => {
		const headers = ['a'.repeat(301), 'b'.repeat(302), 'c'.repeat(303)];
		const expected = {
			['a'.repeat(301)]: 'a'.repeat(300),
			['b'.repeat(302)]: 'b'.repeat(300),
			['c'.repeat(303)]: 'c'.repeat(300)
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});

	test('special chars', () => {
		const headers = ['name@domain', 'user!profile', 'location#home'];
		const expected = {
			'name@domain': 'name_domain',
			'user!profile': 'user_profile',
			'location#home': 'location_home'
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});

	test('case sensitivity', () => {
		const headers = ['Name', 'NAME', 'name'];
		const expected = {
			'Name': 'Name',
			'NAME': 'NAME',
			'name': 'name'
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});

	test('mixed types', () => {
		const headers = ['age', '123Age', 'age123'];
		const expected = {
			'age': 'age',
			'123Age': '_123Age',
			'age123': 'age123'
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});

	test('no input', () => {
		const headers = [];
		const expected = {};
		expect(prepHeaders(headers)).toEqual(expected);
	});

	test('detailed array', () => {
		const headers = ['name', 'NAME', 'name_1'];
		const expectedArray = [
			['name', 'name'],
			['NAME', 'NAME'],
			['name_1', 'name_1']
		];
		expect(prepHeaders(headers, true)).toEqual(expectedArray);
	});

	test('SQL characters', () => {
		const headers = ['select*', 'from?', 'where!', 'group@', 'order#'];
		const expected = {
			'select*': 'select_',
			'from?': 'from_',
			'where!': 'where_',
			'group@': 'group_',
			'order#': 'order_'
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});

	test('moar specials', () => {
		const headers = ['user%name', 'e-mail', 'full/name', 'role+level', 'date-time'];
		const expected = {
			'user%name': 'user_name',
			'e-mail': 'e_mail',
			'full/name': 'full_name',
			'role+level': 'role_level',
			'date-time': 'date_time'
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});

	test('unicode', () => {
		const headers = ['naïve', 'façade', 'résumé', 'coöperate', 'exposé'];
		const expected = {
			'naïve': 'na_ve',
			'façade': 'fa_ade',
			'résumé': 'r_sum_',
			'coöperate': 'co_perate',
			'exposé': 'expos_'
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});

	test('emoji', () => {
		const headers = ['🚀Launch', 'Profit💰', '✈️Travel'];
		const expected = {
			'🚀Launch': '__Launch',
			'Profit💰': 'Profit__',
			'✈️Travel': '__Travel'
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});

	test('leading numbers', () => {
		const headers = ['1stPlace', '2ndBase', '3rdWheel'];
		const expected = {
			'1stPlace': '_1stPlace',
			'2ndBase': '_2ndBase',
			'3rdWheel': '_3rdWheel'
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});

	test('complex scenarios', () => {
		const headers = ['name@domain.com', 'user-profile', '100%Guaranteed', 'hello_world', 'XMLHttpRequest'];
		const expected = {
			'name@domain.com': 'name_domain_com',
			'user-profile': 'user_profile',
			'100%Guaranteed': '_100_Guaranteed',
			'hello_world': 'hello_world',
			'XMLHttpRequest': 'XMLHttpRequest'
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});


	test('null + undefined', () => {
		const headers = [null, undefined, ''];
		const expected = {
			[null]: "null",
			[undefined]: "undefined",
			empty_index_2: "empty_index_2",
		};
		expect(prepHeaders(headers)).toEqual(expected);
	});


	test('spaces + specials', () => {
		expect(cleanName('first name')).toBe('first_name');
		expect(cleanName('last-name')).toBe('last_name');
		expect(cleanName('email@address.com')).toBe('email_address_com');
	});

	test('number removal', () => {
		expect(cleanName('123Start')).toBe('_start');
		expect(cleanName('!@#$%^&*()')).toBe('db_');
	});

	test('reserved keywords', () => {
		expect(cleanName('select')).toBe('db_select');
		expect(cleanName('table')).toBe('db_table');
	});

	test('minimum length', () => {
		expect(cleanName('ab')).toBe('db_ab');
	});

	test('maximum length', () => {
		const longName = 'a'.repeat(310);
		expect(cleanName(longName).length).toBe(300);
	});

	test('collapse', () => {
		expect(cleanName('name______domain')).toBe('name_domain');
		expect(cleanName('user!!!profile')).toBe('user_profile');
	});

	test('casing', () => {
		expect(cleanName('Name')).toBe('name');
		expect(cleanName('NAME')).toBe('name');
	});

	test('unicode', () => {
		expect(cleanName('naïve')).toBe('na_ve');
		expect(cleanName('résumé')).toBe('r_sum');
	});

	test('emojis', () => {
		expect(cleanName('🚀Launch')).toBe('_launch');
		expect(cleanName('Profit💰')).toBe('profit');
	});

	test('sql escape', () => {
		expect(cleanName('select*')).toBe('db_select');
		expect(cleanName('from?')).toBe('from');
	});

	test('special characters', () => {
		expect(cleanName('user%name')).toBe('user_name');
		expect(cleanName('e-mail')).toBe('e_mail');
		expect(cleanName('full/name')).toBe('full_name');
		expect(cleanName('role+level')).toBe('role_level');
		expect(cleanName('date-time')).toBe('date_time');
	});

	test('nulls', () => {
		expect(cleanName('')).toBe('db_');
		expect(cleanName(null).startsWith('db_unknown')).toBe(true); // Assuming function handles null input
		expect(cleanName(undefined).startsWith('db_unknown')).toBe(true); // Assuming function handles undefined input
	});

	test('field scenarios', () => {
		expect(cleanName('name@domain.com')).toBe('name_domain_com');
		expect(cleanName('user-profile')).toBe('user_profile');
		expect(cleanName('100%Guaranteed')).toBe('_guaranteed');
		expect(cleanName('hello_world')).toBe('hello_world');
		expect(cleanName('XMLHttpRequest')).toBe('xmlhttprequest');
	});

	test('leading numbers', () => {
		expect(cleanName('1stPlace')).toBe('_stplace');
		expect(cleanName('2ndBase')).toBe('_ndbase');
		expect(cleanName('3rdWheel')).toBe('_rdwheel');
	});
});