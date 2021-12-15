package org.sap.commercemigration;

import de.hybris.platform.core.Registry;
import de.hybris.platform.jalo.JaloSession;
import de.hybris.platform.util.RedeployUtilities;
import de.hybris.platform.util.Utilities;

/**
 * Demonstration of how to write a standalone application that can be run
 * directly from within eclipse or from the commandline.<br>
 * To run this from commandline, just use the following command:<br>
 * <code>
 * java -jar bootstrap/bin/ybootstrap.jar "new org.sap.commercemigration.CommercemigrationStandalone().run();"
 * </code> From eclipse, just run as Java Application. Note that you maybe need
 * to add all other projects like ext-commerce, ext-pim to the Launch
 * configuration classpath.
 */
public class CommercemigrationStandalone {
	/**
	 * Main class to be able to run it directly as a java program.
	 *
	 * @param args
	 *            the arguments from commandline
	 */
	public static void main(final String[] args) {
		new CommercemigrationStandalone().run();
	}

	public void run() {
		Registry.activateStandaloneMode();
		Registry.activateMasterTenant();

		final JaloSession jaloSession = JaloSession.getCurrentSession();
		System.out.println("Session ID: " + jaloSession.getSessionID()); // NOPMD
		System.out.println("User: " + jaloSession.getUser()); // NOPMD
		Utilities.printAppInfo();

		RedeployUtilities.shutdown();
	}
}
