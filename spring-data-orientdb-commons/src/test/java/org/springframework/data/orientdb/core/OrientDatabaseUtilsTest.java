package org.springframework.data.orientdb.core;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.orientdb.transaction.OrientTransactionManager;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

/**
 * 
 * @author Matej Zachar
 *
 */
public class OrientDatabaseUtilsTest {

	private OrientDatabaseFacotry<ODatabaseRecord> factory;
	private ODatabaseRecord database;
	
	private OrientTransactionManager transactionManager;
	
	@Before
	@SuppressWarnings("unchecked")
	public void setUp() throws Exception {
		database = mock(ODatabaseRecord.class);
		factory = mock(OrientDatabaseFacotry.class);
		
		transactionManager = new OrientTransactionManager();
		transactionManager.setFactory(factory);
	}

	@Test
	public void getDatabase_noTransaction() {
		OrientDatabaseUtils.getDatabase(factory);
		OrientDatabaseUtils.getDatabase(factory);
		
		verify(factory, times(2)).getDatabase();
	}
	
	@Test
	public void getDatabase_transaction() {
		when(factory.getDatabase()).thenReturn(database);
		
		transactionManager.getTransaction(null);
		verify(factory).getDatabase(); // its called from txManager
		
		OrientDatabaseUtils.getDatabase(factory);
		
		// was still called only once from txManager
		verify(factory).getDatabase();
	}
	
	@Test
	public void releaseDatabase_notTransaction() {
		when(factory.getDatabase()).thenReturn(database);
		
		ODatabaseRecord db1 = OrientDatabaseUtils.getDatabase(factory);
		verify(factory).getDatabase();
		
		OrientDatabaseUtils.releaseConnection(db1, factory);
		verify(database).close();
	}
	
	@Test
	public void releaseDatabase_transaction() {
		when(factory.getDatabase()).thenReturn(database);
		
		transactionManager.getTransaction(null);
		verify(factory).getDatabase(); // its called from txManager
		
		ODatabaseRecord db1 = OrientDatabaseUtils.getDatabase(factory);
		verify(factory).getDatabase(); // was still called only once from txManager
		
		OrientDatabaseUtils.releaseConnection(db1, factory);
		verify(database, times(0)).close(); // close cannot be called on transaction managed database connection
	}

}
