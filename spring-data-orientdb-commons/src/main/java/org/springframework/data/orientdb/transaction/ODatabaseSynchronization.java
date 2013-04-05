package org.springframework.data.orientdb.transaction;

import org.springframework.data.orientdb.core.OrientDatabaseFacotry;
import org.springframework.data.orientdb.core.OrientDatabaseUtils;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

/**
 * Callback for resource cleanup at the end of a transaction.
 * When participating in non-native OrientDB transaction (e.g.: when used within {@link JtaTransactionManager})
 * 
 * @author Matej Zachar
 * 
 */
public class ODatabaseSynchronization<DB extends ODatabaseRecord> extends TransactionSynchronizationAdapter {

	private final ODatabaseHolder<DB> dbHolder;
	private final OrientDatabaseFacotry<DB> factory;

	public ODatabaseSynchronization(ODatabaseHolder<DB> dbHolder, OrientDatabaseFacotry<DB> factory) {
		this.dbHolder = dbHolder;
		this.factory = factory;
	}

	@Override
	public void afterCompletion(int status) {
		// The thread-bound ODatabaseHolder might not be available anymore,
		// since afterCompletion might get called from a different thread.
		TransactionSynchronizationManager.unbindResourceIfPossible(factory);
		
		DB db = dbHolder.getDatabase();
		
		dbHolder.clear();
		
		OrientDatabaseUtils.releaseConnection(db, factory);
	}

}
