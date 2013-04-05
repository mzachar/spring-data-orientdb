package org.springframework.data.orientdb.mock;

import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.orientdb.core.AbstractOrientDatabaseFactory;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

public class OrientDocumentDatabaseFactoryMock extends AbstractOrientDatabaseFactory<ODatabaseDocumentTx> {
	
	public OrientDocumentDatabaseFactoryMock(String uri, String user, String password) {
		super(uri, user, password);
	}

	@Override
	protected ODatabaseDocumentTx doGetDatabase(String uri, UserCredentials credentials) {
		ODatabaseDocumentTx db = new ODatabaseDocumentTx(uri);
		if (db.exists()) {
	    	db.open(credentials.getUsername(), credentials.getPassword());
	    } else {
	    	db.create();
	    }
		return db;
	}
	
}
