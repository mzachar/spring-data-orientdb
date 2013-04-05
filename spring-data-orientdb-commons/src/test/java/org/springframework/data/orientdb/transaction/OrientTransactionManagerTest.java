package org.springframework.data.orientdb.transaction;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.data.orientdb.core.OrientDatabaseUtils;
import org.springframework.data.orientdb.mock.OrientDocumentDatabaseFactoryMock;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction;

public class OrientTransactionManagerTest {

	private OrientDocumentDatabaseFactoryMock factory = new OrientDocumentDatabaseFactoryMock("memory:testDb", "admin", "admin");
	
	private ODatabaseDocumentTx db;
	
	private OrientTransactionManager transactionManager;
	
	@Before
	public void setUp() throws Exception {
		db = factory.getDatabase();
		
		transactionManager = new OrientTransactionManager();
		transactionManager.setFactory(factory);
	}

	@After
	public void tearDown() throws Exception {
		if (!db.isClosed()) {
			if (db.getTransaction().isActive()) 
				db.rollback();
			
			if(db.exists()) 
		    	db.drop();
			
			db.close();
		}
		
	}

	@Test
	public void testDoGetTransaction() {
		Object txObject = transactionManager.doGetTransaction();
		assertTrue(txObject instanceof OrientTransactionObject);
	}

	@Test
	public void testDoBegin() {
		OrientTransactionObject txObject = (OrientTransactionObject) transactionManager.doGetTransaction();
		transactionManager.doBegin(txObject, null);
		
		assertTrue(txObject.getDatabaseHolder().isTransactionActive());
		assertTrue(txObject.getDatabaseHolder().isSynchronizedWithTransaction());
		
		assertTrue(txObject.getDatabaseHolder().getDatabase().getTransaction().isActive());
	}

	@Test
	public void testCloseDatabaseConnectionAfterFailedBegin() {
		OrientTransactionObject txObject = (OrientTransactionObject) transactionManager.doGetTransaction();
		transactionManager.doBegin(txObject, null);
		
		ODatabaseRecord db = txObject.getDatabaseHolder().getDatabase();
		OTransaction tx = db.getTransaction();
		assertTrue(tx.isActive());
		transactionManager.closeDatabaseConnectionAfterFailedBegin(txObject);

		assertTrue(!tx.isActive());
		assertTrue(db.isClosed());
	}

	@Test
	public void testDoCommit() {
		OrientTransactionObject txObject = (OrientTransactionObject) transactionManager.doGetTransaction();
		
		// Creates the class before beginning the transaction
		db.getMetadata().getSchema().createClass("Test");
		
		transactionManager.doBegin(txObject, null);

		ODatabaseDocumentTx db = (ODatabaseDocumentTx) txObject.getDatabaseHolder().getDatabase();
		OTransaction tx = db.getTransaction();
		
		ODocument doc = new ODocument("Test");
		doc.field("testkey", "testvalue");
		db.save(doc);
		
		doc = new ODocument("Test");
		doc.field("testkey", "testvalue2");
		db.save(doc);
		
		DefaultTransactionStatus status = Mockito.mock(DefaultTransactionStatus.class);
		when(status.getTransaction()).thenReturn(txObject);
		when(status.isDebug()).thenReturn(true);
		
		transactionManager.doCommit(status);
		
		assertTrue(!tx.isActive());

		db.rollback();
		
		assertTrue (db.countClass("Test") == 2);
		
		for (ODocument document : db.browseClass("Test")) {
			  assertTrue(document.field( "testkey" ).toString().startsWith("testvalue"));
		}
	}

	@Test
	public void testDoRollback() {
		OrientTransactionObject txObject = (OrientTransactionObject) transactionManager.doGetTransaction();
		
		// Creates the class before beginning the transaction
		db.getMetadata().getSchema().createClass("Test");
		
		transactionManager.doBegin(txObject, null);

		ODatabaseDocumentTx db = (ODatabaseDocumentTx) txObject.getDatabaseHolder().getDatabase();
		OTransaction tx = db.getTransaction();
		
		ODocument doc = new ODocument("Test");
		doc.field("testkey", "testvalue");
		db.save(doc);
		
		doc = new ODocument("Test");
		doc.field("testkey", "testvalue2");
		db.save(doc);
		
		DefaultTransactionStatus status = Mockito.mock(DefaultTransactionStatus.class);
		when(status.getTransaction()).thenReturn(txObject);
		when(status.isDebug()).thenReturn(true);
		
		transactionManager.doRollback(status);
		
		assertTrue(!tx.isActive());

		assertTrue (db.countClass("Test") == 0);
	}
	
	@Test
	public void testDoSuspend() {
		OrientTransactionObject txObject = (OrientTransactionObject) transactionManager.doGetTransaction();
		transactionManager.doBegin(txObject, null);
		
		ODatabaseHolder<? extends ODatabaseRecord> holder = txObject.getDatabaseHolder();
		
		Object suspendedResourceHolder = transactionManager.doSuspend(txObject);

		// make sure that holder is the same object as suspendedResourceHolder
		assertTrue(holder == suspendedResourceHolder);
		
		// make sure its removed from current txObject
		assertNull(txObject.getDatabaseHolder());
		
		// make sure its unbind
		assertNull(TransactionSynchronizationManager.getResource(factory));
	}
	
	@Test
	public void testDoResume() {
		OrientTransactionObject txObject = new OrientTransactionObject();
		ODatabaseHolder<ODatabaseRecord> suspendedResource = new ODatabaseHolder<ODatabaseRecord>(factory.getDatabase());
		
		transactionManager.doResume(txObject, suspendedResource);

		// make sure the suspended resource will get rebind
		assertTrue(TransactionSynchronizationManager.getResource(factory) == suspendedResource);
	}

	@Test
	public void testDoCleanupAfterCompletionObject() {
		OrientTransactionObject txObject = (OrientTransactionObject) transactionManager.doGetTransaction();
		transactionManager.doBegin(txObject, null);
		
		ODatabaseRecord db = txObject.getDatabaseHolder().getDatabase();
		OTransaction tx = db.getTransaction();
		assertTrue(tx.isActive());
		
		DefaultTransactionStatus status = Mockito.mock(DefaultTransactionStatus.class);
		when(status.getTransaction()).thenReturn(txObject);
		when(status.isDebug()).thenReturn(true);
		
		transactionManager.doRollback(status);
		
		transactionManager.doCleanupAfterCompletion(txObject);

		assertTrue(!tx.isActive());
		assertTrue(!txObject.getDatabaseHolder().isSynchronizedWithTransaction());
		assertTrue(!txObject.getDatabaseHolder().isTransactionActive());
		assertTrue(db.isClosed());
	}
	
	@Test
	public void testPropagation_requiresNew() {
		ODocument d1 = null;
		ODocument d2 = null;
				
		// Creates the class before beginning the transactions
		db.getMetadata().getSchema().createClass("Test");
				
		// 1. transaction
		TransactionStatus status1 = transactionManager.getTransaction(new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_REQUIRES_NEW));
		ODatabaseDocumentTx db1 = OrientDatabaseUtils.getDatabase(factory);
		d1 = new ODocument("Test");
		db1.save(d1);
		
		{
			// 2. transaction (nested)
			TransactionStatus status2 = transactionManager.getTransaction(new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_REQUIRES_NEW));
			ODatabaseDocumentTx db2 = OrientDatabaseUtils.getDatabase(factory);
			assertTrue(db1 != db2); // have to be different instance
			assertThat(db.browseClass("Test")).hasSize(0);
		
			d2 = new ODocument("Test");
			d2.field("db2", "tx2");
			db2.save(d2);
			transactionManager.commit(status2);
		}
		
		// we have to see outcome of nested transaction
		assertThat(db.browseClass("Test")).containsExactly(d2);
		
		// commit 1. tx
		transactionManager.commit(status1);
		
		// we have to end up with both documents in the class
		assertThat(db.browseClass("Test")).containsExactly(d1, d2);
	}

}
