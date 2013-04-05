package org.springframework.data.orientdb.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.orientdb.CannotGetOriendDbConnectionException;
import org.springframework.data.orientdb.transaction.ODatabaseHolder;
import org.springframework.data.orientdb.transaction.ODatabaseProxy;
import org.springframework.data.orientdb.transaction.ODatabaseSynchronization;
import org.springframework.data.orientdb.transaction.OrientTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

/**
 * Helper class that provides static methods for obtaining database from a
 * {@link OrientDatabaseFacotry}. Includes special support for Spring-managed
 * transactional databases, e.g. managed by {@link OrientTransactionManager} or
 * {@link org.springframework.transaction.jta.JtaTransactionManager}.
 * 
 * <p>
 * Can also be used directly in application code as low level API.
 * 
 * @author Matej Zachar
 * 
 */
public class OrientDatabaseUtils {

	private static final Log logger = LogFactory.getLog(OrientDatabaseUtils.class);

	/**
	 * Obtain a database from the given factory. Translates OExceptions into the
	 * Spring hierarchy of unchecked generic data access exceptions, simplifying
	 * calling code and making any exception that is thrown more meaningful.
	 * <p>
	 * Is aware of a corresponding database bound to the current thread, for
	 * example when using {@link OrientTransactionManager}. Will bind a
	 * database to the thread if transaction synchronization is active, e.g.
	 * when running within a
	 * {@link org.springframework.transaction.jta.JtaTransactionManager JTA}
	 * transaction).
	 * 
	 * @param factory
	 *            the {@link OrientDatabaseFacotry} to obtain database from
	 * @return orient database from the given factory
	 * @throws CannotGetOriendDbConnectionException
	 *             if the attempt to get a database failed
	 * 
	 * @see #releaseConnection
	 */
	public static <DB extends ODatabaseRecord> DB getDatabase(OrientDatabaseFacotry<DB> factory) throws CannotGetOriendDbConnectionException {
		try {
			return doGetDatabase(factory);
		} catch (OException ex) {
			throw new CannotGetOriendDbConnectionException("Could not get JDBC Connection", ex);
		}
	}

	/**
	 * Actually obtain a database from the given factory Same as
	 * {@link #getDatabase(OrientDatabaseFacotry)}, but throwing the original
	 * {@link OException}.
	 * <p>
	 * Is aware of a corresponding database bound to the current thread, for
	 * example when using {@link OrientTransactionManager}. Will bind a database
	 * to the thread if transaction synchronization is active (e.g. if in a JTA
	 * transaction).
	 * 
	 * @param factory
	 *            the {@link OrientDatabaseFacotry} to obtain database from
	 * @return orient database from the given factory
	 * @throws OException
	 *             if thrown by OrientDB commands
	 * @see #doReleaseConnection
	 */
	public static <DB extends ODatabaseRecord> DB doGetDatabase(OrientDatabaseFacotry<DB> factory) throws OException {
		@SuppressWarnings("unchecked")
		ODatabaseHolder<DB> dbHolder = (ODatabaseHolder<DB>) TransactionSynchronizationManager.getResource(factory);
		if (dbHolder != null && dbHolder.isSynchronizedWithTransaction()) {
			dbHolder.requested();
			logger.debug("Fetching bound database for current transaction");
			return dbHolder.getDatabase();
		}
		
		logger.debug("Fetchin new database from factory");
		DB db = factory.getDatabase();
		
		if (TransactionSynchronizationManager.isSynchronizationActive()) {
			logger.debug("Registering transaction synchronization for fetched database");
			dbHolder = new ODatabaseHolder<DB>(db);
			
			TransactionSynchronizationManager.registerSynchronization(new ODatabaseSynchronization<DB>(dbHolder, factory));
			dbHolder.setSynchronizedWithTransaction(true);
			
			TransactionSynchronizationManager.bindResource(factory, dbHolder);
		}
		
		return db;
	}

	/**
	 * Close the given database, obtained from the given factory, if it is not
	 * managed externally (that is, not bound to the thread).
	 * 
	 * @param database
	 *            the database to close if necessary (if this is
	 *            <code>null</code>, the call will be ignored)
	 * @param factory
	 *            the factory that the database was obtained from (may be
	 *            <code>null</code>)
	 * @see #getDatabase(OrientDatabaseFacotry)
	 */
	public static <DB extends ODatabaseRecord> void releaseConnection(DB database, OrientDatabaseFacotry<DB> factory) {
		try {
			doReleaseConnection(database, factory);

		} catch (OException e) {
			logger.debug("Could not close database", e);

		} catch (Throwable e) {
			logger.debug("Unexpected exception on closing database", e);
		}
	}

	/**
	 * Actually close the given database, obtained from the given factory. Same
	 * as {@link #releaseConnection}, but throwing the original OException.
	 * 
	 * @param database
	 *            the database to close if necessary (if this is
	 *            <code>null</code>, the call will be ignored)
	 * @param facotry
	 *            the factory that the database was obtained from (may be
	 *            <code>null</code>)
	 * @throws OException
	 *             if thrown by OrientDB methods
	 * @see #doGetConnection
	 */
	public static <DB extends ODatabaseRecord> void doReleaseConnection(DB database, OrientDatabaseFacotry<DB> factory) throws OException {
		if (database == null) {
			return;
		}

		if (factory != null) {
			@SuppressWarnings("unchecked")
			ODatabaseHolder<DB> dbHolder = (ODatabaseHolder<DB>) TransactionSynchronizationManager.getResource(factory);
			if (dbHolder != null && connectionEquals(dbHolder, database)) {
				// It's the transactional database: Don't close it.
				dbHolder.released();
				return;
			}
		}

		database.close();
	}

	/**
	 * Determine whether the given two database are equal, asking the target
	 * database in case of a proxy. Used to detect equality even if the user
	 * passed in a raw target database while the held one is a proxy.
	 * 
	 * @param dbHolder
	 *            the {@link ODatabaseHolder} for the held database (potentially
	 *            a proxy)
	 * @param passedInDatabase
	 *            the database passed-in by the user (potentially a target
	 *            database without proxy)
	 * @return whether the given databases are equal
	 * @see #getTargetConnection
	 */
	private static <DB extends ODatabaseRecord> boolean connectionEquals(ODatabaseHolder<DB> dbHolder, DB passedInDatabase) {
		DB heldDatabase = dbHolder.getDatabase();
		return (heldDatabase == passedInDatabase //
				|| heldDatabase.equals(passedInDatabase) //
		|| getTargetDatabase(heldDatabase).equals(passedInDatabase));
	}

	/**
	 * Return the innermost target database of the given database. If the
	 * given database is a proxy, it will be unwrapped until a non-proxy
	 * database is found. Otherwise, the passed-in database will be returned
	 * as-is.
	 * 
	 * @param database
	 *            the database proxy to unwrap
	 * @return the innermost target database, or the passed-in one if no proxy
	 * @see ODatabaseProxy#getTargetDatabase()
	 */
	public static ODatabaseRecord getTargetDatabase(ODatabaseRecord database) {
		ODatabaseRecord dbToUse = database;
		while (dbToUse instanceof ODatabaseProxy) {
			dbToUse = ((ODatabaseProxy) dbToUse).getTargetDatabase();
		}
		return dbToUse;
	}

}
