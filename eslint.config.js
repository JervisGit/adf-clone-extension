module.exports = {
	languageOptions: {
		ecmaVersion: 2022,
		sourceType: "module",
		globals: {
			require: "readonly",
			module: "readonly",
			exports: "readonly",
			process: "readonly",
			console: "readonly",
			__dirname: "readonly",
			__filename: "readonly",
			setImmediate: "readonly",
			setTimeout: "readonly",
			clearTimeout: "readonly",
			setInterval: "readonly",
			clearInterval: "readonly",
			suite: "readonly",
			test: "readonly",
			fetch: "readonly",
			window: "readonly",
			JSON: "readonly",
			Array: "readonly",
			Buffer: "readonly"
		}
	},
	rules: {
		"no-unused-vars": "warn",
		"no-undef": "error"
	}
};
