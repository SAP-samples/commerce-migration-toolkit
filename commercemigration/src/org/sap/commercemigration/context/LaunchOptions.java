/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.context;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class LaunchOptions {

	public static final LaunchOptions NONE = new LaunchOptions();

	private Map<String, Serializable> propertyOverrideMap;

	public LaunchOptions() {
		this.propertyOverrideMap = new HashMap<>();
	}

	public Map<String, Serializable> getPropertyOverrideMap() {
		return propertyOverrideMap;
	}
}
