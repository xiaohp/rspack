module.exports = {
	module: {
		rules: [
			{
				test: /\.s[ac]ss$/i,
				use: [{ builtinLoader: "sass-loader" }],
				type: "css"
			}
		]
	},
	stats: "errors-warnings"
};
