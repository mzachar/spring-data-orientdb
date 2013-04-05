package org.springframework.data.orientdb.transaction;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.orientdb.core.OrientDatabaseUtils;
import org.springframework.data.orientdb.mock.OrientDocumentDatabaseFactoryMock;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;


/**
 * 
 * @author "Forat Latif"
 *
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
//@TransactionConfiguration(transactionManager="orientTransactionManager", defaultRollback=false)
//@Transactional
public class TransactionalAnnotationTest {
	
	@Autowired
	private OrientDocumentDatabaseFactoryMock factory;
	
	@Autowired
	private TransactionalMethodsContainer txMethodsContainer;
	
	private ODatabaseDocumentTx db;
	
	@Before
	public void setUp() {
		db = OrientDatabaseUtils.getDatabase(factory);
	}
	
	@After
	public void tearDown() {
		db = OrientDatabaseUtils.getDatabase(factory);
		if(db.exists()) {
	    	db.drop();
	    }
		db.close();
	}
	
	@Test
	public void commitWithAnnotationTest() {
		assertTrue(!db.getTransaction().isActive());
		db.getMetadata().getSchema().createClass("testcommit");
		txMethodsContainer.commitAutomatically("testcommit");
		assertTrue(!db.getTransaction().isActive());
		assertTrue(db.isClosed());
		
		db = OrientDatabaseUtils.getDatabase(factory);
		assertTrue(db.countClass("testcommit") == 1);
	}
	
	@Test
	public void rollbackWithAnnotationTest() {
		assertTrue(!db.getTransaction().isActive());
		db.getMetadata().getSchema().createClass("testrollback");
		try {
			txMethodsContainer.rollbackOnError("testrollback");
		}
		catch (Exception e) {
			
		}
		assertTrue(!db.getTransaction().isActive());
		assertTrue(db.isClosed());
		
		db = OrientDatabaseUtils.getDatabase(factory);
		
		assertTrue(db.countClass("testrollback") == 0);
	}

}
