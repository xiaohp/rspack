module.exports = {
	mode: "production",
	optimization: {
		minimize: false,
		usedExports: true
	},
	experiments: {
		rspackFuture: {
			newTreeshaking: true
		}
	}
};
