package org.springframework.data.orientdb.core;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

/**
 * Factory which creates {@link ODatabaseRecord database}
 * 
 * Do not implement this interface directly rather use {@link AbstractOrientDatabaseFactory}
 * 
 * @author Matej Zachar
 * 
 */
public interface OrientDatabaseFacotry<DB extends ODatabaseRecord> {

	/**
	 * Obtains database connection.
	 * Database has to be always closed when no longer needed.
	 * e.g.:
	 * <pre>
	 * ODatabaseRecord db = factory.getDatabase();
	 * try {
	 * 	// ... use db here
	 * } finally {
	 * 	db.close();
	 * }
	 * </pre>
	 * 
	 * @return database
	 */
	public DB getDatabase();

}
