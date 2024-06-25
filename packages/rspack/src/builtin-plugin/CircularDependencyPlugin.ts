import {
	BuiltinPluginName,
	RawCircularDependencyPluginOptions
} from "@rspack/binding";

import { create } from "./base";

export type CircularDependencyPluginOptions = {
	exclude?: RegExp;
	include?: RegExp;
	failOnError?: boolean;
	allowAsyncCycles?: boolean;
	onDetected?: boolean;
	cwd?: string;
};

export const CircularDependencyPlugin = create(
	BuiltinPluginName.CircularDependencyPlugin,
	(
		options: CircularDependencyPluginOptions
	): RawCircularDependencyPluginOptions => {
		return options;
	}
);
