package org.springframework.data.orientdb.transaction;

import org.springframework.transaction.support.ResourceHolderSupport;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

public class ODatabaseHolder<DB extends ODatabaseRecord> extends ResourceHolderSupport  {

	private DB database;
	
	private boolean transactionActive = false;
	
	public ODatabaseHolder(DB database) {
		this.database = database;
	}
	
	public DB getDatabase() {
		return database;
	}

	public boolean isTransactionActive() {
		return transactionActive;
	}
	
	public void setTransactionActive(boolean transactionActive) {
		this.transactionActive = transactionActive;
	}
	
	@Override
	public void clear() {
		super.clear();
		database = null;
		transactionActive = false;
	}
	
}
