package org.springframework.data.orientdb.transaction;

import static org.junit.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.orientdb.core.OrientDatabaseUtils;
import org.springframework.data.orientdb.mock.OrientDocumentDatabaseFactoryMock;
import org.springframework.transaction.annotation.Transactional;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class TransactionalMethodsContainer {

	@Autowired
	private OrientDocumentDatabaseFactoryMock factory;
	
	@Transactional
	public void commitAutomatically(String className) {
		ODatabaseDocumentTx db = OrientDatabaseUtils.getDatabase(factory);
		assertTrue(db.getTransaction().isActive());
		
		ODocument doc = new ODocument(className);
		doc.field("test", "test");
		db.save(doc);
		
	}
	
	@Transactional
	public void rollbackOnError(String className) {
		ODatabaseDocumentTx db = OrientDatabaseUtils.getDatabase(factory);
		assertTrue(db.getTransaction().isActive());
		ODocument doc = new ODocument(className);
		doc.field("test", "test");
		db.save(doc);
		
		throw new RuntimeException();
	}
	
}
