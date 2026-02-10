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
			suite: "readonly",
			test: "readonly",
			fetch: "readonly"
		}
	},
	rules: {
		"no-unused-vars": "warn",
		"no-undef": "error"
	}
};
