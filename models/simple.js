const Chance = require('chance');
const chance = new Chance();
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
dayjs.extend(utc);
const { uid, comma } = require('ak-tools');


/** @type {import('make-mp-data').Config} */
const config = {
	token: "",
	seed: "foo bar baz",
	numDays: 30, //how many days worth of data
	numEvents: 1000, //how many events
	numUsers: 100, //how many users	
	format: 'csv', //csv or json
	region: "US",
	anonIds: true, //if true, anonymousIds are created for each user
	sessionIds: true, //if true, sessionIds are created for each user

	events: [
		{
			"event": "checkout",
			"weight": 2,
			"properties": {
				amount: weightedRange(5, 500, 1000, .25),
				currency: ["USD", "CAD", "EUR", "BTC", "ETH", "JPY"],
			}
		},
		{
			"event": "add to cart",
			"weight": 4,
			"properties": {
				isFeaturedItem: [true, false, false],
				amount: weightedRange(5, 500, 1000, .25),
				rating: weightedRange(1, 5),
				reviews: weightedRange(0, 35),
				product_id: weightedRange(1, 1000)
			}
		},
		{
			"event": "page view",
			"weight": 10,
			"properties": {
				page: ["/", "/", "/help", "/account", "/watch", "/listen", "/product", "/people", "/peace"],
				utm_source: ["$organic", "$organic", "$organic", "$organic", "google", "google", "google", "facebook", "facebook", "twitter", "linkedin"],
			}
		},
		{
			"event": "watch video",
			"weight": 8,
			"properties": {
				category: ["funny", "educational", "inspirational", "music", "news", "sports", "cooking", "DIY", "travel", "gaming"],
				watchTimeSec: weightedRange(10, 600, 1000, .25),
				quality: ["2160p", "1440p", "1080p", "720p", "480p", "360p", "240p"],
				format: ["mp4", "avi", "mov", "mpg"],
				uploader_id: chance.guid.bind(chance)

			}
		},
		{
			"event": "view item",
			"weight": 8,
			"properties": {
				product_id: weightedRange(1, 1000),
				colors: ["light", "dark", "custom", "dark"]
			}
		},
		{
			"event": "save item",
			"weight": 5,
			"properties": {
				product_id: weightedRange(1, 1000),
				colors: ["light", "dark", "custom", "dark"]
			}
		},
		{
			"event": "sign up",
			"isFirstEvent": true,
			"weight": 0,
			"properties": {
				variants: ["A", "B", "C", "Control"],
				flows: ["new", "existing", "loyal", "churned"],
				flags: ["on", "off"],
				experiment_ids: ["1234", "5678", "9012", "3456", "7890"],
				multiVariate: [true, false]
			}
		}
	],
	superProps: {
		platform: ["web", "mobile", "web", "mobile", "web", "kiosk", "smartTV"],
		// emotions: generateEmoji(),

	},
	/*
	user properties work the same as event properties
	each key should be an array or function reference
	*/
	userProps: {
		title: chance.profession.bind(chance),
		luckyNumber: weightedRange(42, 420),
		// vibe: generateEmoji(),
		spiritAnimal: chance.animal.bind(chance)
	},

	scdProps: {
		plan: ["free", "free", "free", "free", "basic", "basic", "basic", "premium", "premium", "enterprise"],
		MRR: weightedRange(0, 10000, 1000, .15),
		NPS: weightedRange(0, 10, 150, 2),
		marketingOptIn: [true, true, false],
		dateOfRenewal: date(100, false),
	},

	/*
	for group analytics keys, we need an array of arrays [[],[],[]] 
	each pair represents a group_key and the number of profiles for that key
	*/
	groupKeys: [

	],
	groupProps: {
		
	},

	lookupTables: [
		{
			key: "product_id",
			entries: 1000,
			attributes: {
				category: [
					"Books",
					"Movies",
					"Music",
					"Games",
					"Electronics",
					"Computers",
					"Smart Home",
					"Home",
					"Garden & Tools",
					"Pet Supplies",
					"Food & Grocery",
					"Beauty",
					"Health",
					"Toys",
					"Kids",
					"Baby",
					"Handmade",
					"Sports",
					"Outdoors",
					"Automotive",
					"Industrial",
					"Entertainment",
					"Art"
				],
				"demand": ["high", "medium", "medium", "low"],
				"supply": ["high", "medium", "medium", "low"],
				"manufacturer": chance.company.bind(chance),
				"price": weightedRange(5, 500, 1000, .25),
				"rating": weightedRange(1, 5),
				"reviews": weightedRange(0, 35)
			}

		}
	],
};


function pick(val) {
	if (val) return chance.pickone(val);
	try {
		const choice = chance.pickone(this);
		return choice;
	}
	catch (e) {
		return null;
	}
}

function date(inTheLast = 30, isPast = true, format = 'YYYY-MM-DD') {
	const now = dayjs.utc();
	return function () {
		try {
			const when = chance.integer({ min: 0, max: Math.abs(inTheLast) });
			let then;
			if (isPast) {
				then = now.subtract(when, 'day')
					.subtract(integer(0, 23), 'hour')
					.subtract(integer(0, 59), 'minute')
					.subtract(integer(0, 59), 'second');
			}
			if (!isPast) {
				then = now.add(when, 'day')
					.add(integer(0, 23), 'hour')
					.add(integer(0, 59), 'minute')
					.add(integer(0, 59), 'second');
			}
			if (format) return then?.format(format);
			if (!format) return then?.toISOString();
		}
		catch (e) {
			if (format) return now?.format(format);
			if (!format) return now?.toISOString();
		}
	};
}



function integer(min, max) {
	if (min === max) {
		return min;
	}

	if (min > max) {
		return chance.integer({
			min: max,
			max: min
		});
	}

	if (min < max) {
		return chance.integer({
			min: min,
			max: max
		});
	}

	return 0;
}

function makeHashTags() {
	const popularHashtags = [
		'#GalacticAdventures',
		'#EnchantedExplorations',
		'#MagicalMoments',
		'#EpicQuests',
		'#WonderfulWorlds',
		'#FantasyFrenzy',
		'#MysticalMayhem',
		'#MythicalMarvels',
		'#LegendaryLegends',
		'#DreamlandDiaries',
		'#WhimsicalWonders',
		'#FabledFables'
	];

	const numHashtags = integer(integer(1, 5), integer(5, 10));
	const hashtags = [];
	for (let i = 0; i < numHashtags; i++) {
		hashtags.push(chance.pickone(popularHashtags));
	}
	return hashtags;
}

function buildExperiment() {
	const variants = ["A", "B", "C", "Control"];
	const flows = ["new", "existing", "loyal", "churned"];
	const flags = ["on", "off"];
	const experiment_ids = ["1234", "5678", "9012", "3456", "7890"];
	const multiVariate = [true, false];
	const experiment = {
		experiment_id: pick(experiment_ids),
		variant: pick(variants),
		flow: pick(flows),
		multiVariate: pick(multiVariate),
		flag: pick(flags)
	};
	return experiment;

}

function makeProducts() {
	let categories = ["Device Accessories", "eBooks", "Automotive", "Baby Products", "Beauty", "Books", "Camera & Photo", "Cell Phones & Accessories", "Collectible Coins", "Consumer Electronics", "Entertainment Collectibles", "Fine Art", "Grocery & Gourmet Food", "Health & Personal Care", "Home & Garden", "Independent Design", "Industrial & Scientific", "Accessories", "Major Appliances", "Music", "Musical Instruments", "Office Products", "Outdoors", "Personal Computers", "Pet Supplies", "Software", "Sports", "Sports Collectibles", "Tools & Home Improvement", "Toys & Games", "Video, DVD & Blu-ray", "Video Games", "Watches"];
	let slugs = ['/sale/', '/featured/', '/home/', '/search/', '/wishlist/', '/'];
	let assetExtension = ['.png', '.jpg', '.jpeg', '.heic', '.mp4', '.mov', '.avi'];
	let data = [];
	let numOfItems = integer(1, 12);

	for (var i = 0; i < numOfItems; i++) {

		let category = chance.pickone(categories);
		let slug = chance.pickone(slugs);
		let asset = chance.pickone(assetExtension);
		let product_id = chance.guid();
		let price = integer(1, 300);
		let quantity = integer(1, 5);

		let item = {
			product_id: product_id,
			sku: integer(11111, 99999),
			amount: price,
			quantity: quantity,
			value: price * quantity,
			featured: chance.pickone([true, false]),
			category: category,
			urlSlug: slug + category,
			asset: `${category}-${integer(1, 20)}${asset}`
		};

		data.push(item);
	}

	return data;
}

// Box-Muller transform to generate standard normally distributed values
function boxMullerRandom() {
	let u = 0, v = 0;
	while (u === 0) u = Math.random();
	while (v === 0) v = Math.random();
	return Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
}

// Apply skewness to the value
function applySkew(value, skew) {
	if (skew === 1) return value;
	// Adjust the value based on skew
	let sign = value < 0 ? -1 : 1;
	return sign * Math.pow(Math.abs(value), skew);
}

// Map standard normal value to our range
function mapToRange(value, mean, sd) {
	return Math.round(value * sd + mean);
}

function weightedRange(min, max, size = 100, skew = 1) {
	const mean = (max + min) / 2;
	const sd = (max - min) / 4;
	let array = [];

	for (let i = 0; i < size; i++) {
		let normalValue = boxMullerRandom();
		let skewedValue = applySkew(normalValue, skew);
		let mappedValue = mapToRange(skewedValue, mean, sd);

		// Ensure the mapped value is within our min-max range
		if (mappedValue >= min && mappedValue <= max) {
			array.push(mappedValue);
		} else {
			i--; // If out of range, redo this iteration
		}
	}

	return array;
}

module.exports = config;