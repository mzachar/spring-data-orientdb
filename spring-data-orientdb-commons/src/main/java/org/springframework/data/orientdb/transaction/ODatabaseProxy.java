package org.springframework.data.orientdb.transaction;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

/**
 * 
 * @author Matej Zachar
 *
 */
public interface ODatabaseProxy extends ODatabaseRecord {
	
	public ODatabaseRecord getTargetDatabase();

}
