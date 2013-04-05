package org.springframework.data.orientdb.transaction;

import junit.framework.Assert;

import org.springframework.data.orientdb.core.OrientDatabaseFacotry;
import org.springframework.data.orientdb.core.OrientDatabaseUtils;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OTransactionException;

public class OrientTransactionManager extends AbstractPlatformTransactionManager implements ResourceTransactionManager {

	private static final long serialVersionUID = 8014785737708117419L;
	
	private OrientDatabaseFacotry<? extends ODatabaseRecord> factory;
	
	public OrientTransactionManager() {
		setNestedTransactionAllowed(false);
	}
	
	public void setFactory(OrientDatabaseFacotry<? extends ODatabaseRecord> factory) {
		Assert.assertNull("OrientTransactionManager was already initialized with the factory", this.factory);
		this.factory = factory;
	}
	
	public Object getResourceFactory() {
		return factory;
	}

	@Override
	protected boolean isExistingTransaction(Object transaction) {
		OrientTransactionObject txObject = (OrientTransactionObject) transaction;
		return txObject.getDatabaseHolder() != null && txObject.getDatabaseHolder().isTransactionActive();
	}
	
	@Override
	protected Object doGetTransaction() throws TransactionException {
		OrientTransactionObject txObject = new OrientTransactionObject();
		
		@SuppressWarnings("unchecked")
		ODatabaseHolder<? extends ODatabaseRecord> dbHolder = (ODatabaseHolder<? extends ODatabaseRecord>) TransactionSynchronizationManager.getResource(this.factory);
		if (dbHolder != null) {
			logger.debug("Found thread-bound database ["+dbHolder.getDatabase()+"] for OrientDB transaction");
			txObject.setDatabaseHolder(dbHolder);
		}
		
		return txObject;
	}

	@Override
	protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
		OrientTransactionObject txObject = (OrientTransactionObject) transaction;
		try {
			ODatabaseRecord db = null;
			if (txObject.getDatabaseHolder() == null || txObject.getDatabaseHolder().isSynchronizedWithTransaction()) {
				// we need to obtain new connection
				db = factory.getDatabase();
				logger.debug("Aquired new database for OrientDB transaction");
				txObject.setDatabaseHolder(new ODatabaseHolder<ODatabaseRecord>(db));
			}

			// mark this holder as synchronized
			txObject.getDatabaseHolder().setSynchronizedWithTransaction(true);
			db = txObject.getDatabaseHolder().getDatabase();

			// sets transactionActive
			txObject.getDatabaseHolder().setTransactionActive(true);
			
			// begin orient transaction
			txObject.getDatabaseHolder().getDatabase().begin();
			
			// bind the DatabaseHolder to the thread.
			TransactionSynchronizationManager.bindResource(factory, txObject.getDatabaseHolder());
		}
		catch (Exception e) {
			closeDatabaseConnectionAfterFailedBegin(txObject);
			throw new CannotCreateTransactionException("Could not open a OrientDB transaction with Object: " + txObject, e);
		}
	}
	
	protected void closeDatabaseConnectionAfterFailedBegin(OrientTransactionObject txObject) {
			ODatabaseRecord db = txObject.getDatabaseHolder().getDatabase();
			try {
				if (db.getTransaction().isActive()) {
					db.rollback();
				}
			}
			catch (Throwable ex) {
				logger.debug("Could not rollback OrientDB transaction after failed transaction begin", ex);
			}
			finally {
				doCleanupAfterCompletion(txObject);
			}
	}
	

	@Override
	protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
		OrientTransactionObject txObject = (OrientTransactionObject) status.getTransaction();
		if (status.isDebug()) {
			logger.debug("Committing OrientDB transaction on database [" + txObject.getDatabaseHolder().getDatabase() + "]");
		}
		
		try {
			ODatabaseRecord db = txObject.getDatabaseHolder().getDatabase();
			db.commit();
			
		} catch (OTransactionException ex) {
			throw new TransactionSystemException("Could not commit OrientDB transaction", ex);
			
		} catch (RuntimeException ex) {
			// TODO: Translate exception if necessary
			throw ex;
		}
	}

	@Override
	protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
		OrientTransactionObject txObject = (OrientTransactionObject) status.getTransaction();
		if (status.isDebug()) {
			logger.debug("Rolling back OrientDB transaction on DB [" + txObject.getDatabaseHolder().getDatabase() + "]");
		}
		try {
			ODatabaseRecord db = txObject.getDatabaseHolder().getDatabase();
			db.rollback();
		}
		catch (OTransactionException ex) {
			throw new TransactionSystemException("Could not commit OrientDB transaction", ex);
		}
		catch (RuntimeException ex) {
			// TODO: Translate exception if necessary
			throw ex;
		}
	}
	
	@Override
	protected void doSetRollbackOnly(DefaultTransactionStatus status) throws TransactionException {
		OrientTransactionObject txObject = (OrientTransactionObject) status.getTransaction();
		if (status.isDebug()) {
			logger.debug("Setting OrientDB transaction to rollback-only");
		}
		txObject.setRollbackOnly();
	}
	
	@Override
	protected Object doSuspend(Object transaction) throws TransactionException {
		OrientTransactionObject txObject = (OrientTransactionObject) transaction;
		txObject.setDatabaseHolder(null);
		return (ODatabaseHolder<?>) TransactionSynchronizationManager.unbindResource(factory);
	}
	
	@Override
	protected void doResume(Object transaction, Object suspendedResources) throws TransactionException {
		ODatabaseHolder<?> dbHolder = (ODatabaseHolder<?>) suspendedResources;
		TransactionSynchronizationManager.bindResource(factory, dbHolder);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected void doCleanupAfterCompletion(Object transaction) {
		OrientTransactionObject txObject = (OrientTransactionObject) transaction;

		ODatabaseRecord db = txObject.getDatabaseHolder().getDatabase();
		txObject.getDatabaseHolder().clear();
		
		// Remove the DatabaseHolder from the thread.
		TransactionSynchronizationManager.unbindResource(factory);
		
		OrientDatabaseUtils.releaseConnection(db, (OrientDatabaseFacotry<ODatabaseRecord>)factory);
	}
	
}
