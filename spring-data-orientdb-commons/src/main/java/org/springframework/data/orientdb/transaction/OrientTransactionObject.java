package org.springframework.data.orientdb.transaction;

import org.springframework.transaction.support.SmartTransactionObject;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

public class OrientTransactionObject implements SmartTransactionObject {

	private ODatabaseHolder<? extends ODatabaseRecord> databaseHolder;

	private boolean rollbackOnly;
	
	public OrientTransactionObject() {
		this.rollbackOnly = false;
	}

	public void setDatabaseHolder(ODatabaseHolder<? extends ODatabaseRecord> databaseHolder) {
		this.databaseHolder = databaseHolder;
	}

	public ODatabaseHolder<? extends ODatabaseRecord> getDatabaseHolder() {
		return databaseHolder;
	}

	public boolean hasTransaction() {
		return (databaseHolder != null && databaseHolder.isTransactionActive());
	}

	public void setRollbackOnly() {
		rollbackOnly = true;
		databaseHolder.setRollbackOnly();
	}

	public boolean isRollbackOnly() {
		return rollbackOnly || databaseHolder.isRollbackOnly();
	}

	public void flush() {
		databaseHolder.getDatabase().commit();
	}

}