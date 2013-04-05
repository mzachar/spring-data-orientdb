package org.springframework.data.orientdb.core;

import org.springframework.data.authentication.UserCredentials;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

/**
 * Base implementation for different OrientDB databases
 * 
 * @author Matej Zachar
 * 
 */
public abstract class AbstractOrientDatabaseFactory<DB extends ODatabaseRecord> implements OrientDatabaseFacotry<DB> {

	private String uri;

	private UserCredentials credentials;
	
	public AbstractOrientDatabaseFactory(String uri, UserCredentials credentials) {
		this.uri = uri;
		this.credentials = credentials;
	}
	
	public AbstractOrientDatabaseFactory(String uri, String user, String password) {
		this(uri, new UserCredentials(user, password));
	}

	/**
	 * @param uri
	 *            orient database URI to which this factory will be creating
	 *            connections
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}

	/**
	 * @param credentials
	 *            credentials to use while connecting to the database
	 *            {@link #uri}
	 */
	public void setCredentials(UserCredentials credentials) {
		this.credentials = credentials;
	}

	public final DB getDatabase() {
		return doGetDatabase(uri, credentials);
	}

	/**
	 * Obtain specific database. It should be new database every time this
	 * method is called or obtained from pool
	 * 
	 * @param uri
	 * @param credentials
	 * @return new unbound database
	 */
	protected abstract DB doGetDatabase(String uri, UserCredentials credentials);

	// private DB createTransactionAwareWrapper(DB database) {
	// return (DB) Proxy.newProxyInstance(
	// ODatabaseProxy.class.getClassLoader(),
	// new Class[] {ODatabaseProxy.class},
	// new TransactionAwareInvocationHandler(database));
	// }
	//
	// /**
	// * Invocation handler that delegates close calls on {@link
	// ODatabaseRecord} Connections
	// * to {@link AbstractOrientDbFactory} for being aware of thread-bound
	// transactions.
	// */
	// private class TransactionAwareInvocationHandler implements
	// InvocationHandler {
	//
	// private final DB target;
	//
	// private boolean closed = false;
	//
	// public TransactionAwareInvocationHandler(DB target) {
	// this.target = target;
	// }
	//
	// public Object invoke(Object proxy, Method method, Object[] args) throws
	// Throwable {
	// // Invocation on ConnectionProxy interface coming in...
	//
	// if (method.getName().equals("equals")) {
	// // Only considered as equal when proxies are identical.
	// return (proxy == args[0]);
	// }
	// else if (method.getName().equals("hashCode")) {
	// // Use hashCode of Connection proxy.
	// return System.identityHashCode(proxy);
	// }
	// else if (method.getName().equals("toString")) {
	// // Allow for differentiating between the proxy and the raw DB.
	// StringBuilder sb = new
	// StringBuilder("Transaction-aware proxy for target database ");
	// sb.append("[").append(this.target.toString()).append("]");
	// return sb.toString();
	// }
	// else if (method.getName().equals("unwrap")) {
	// if (((Class) args[0]).isInstance(proxy)) {
	// return proxy;
	// }
	// }
	// else if (method.getName().equals("isWrapperFor")) {
	// if (((Class) args[0]).isInstance(proxy)) {
	// return true;
	// }
	// }
	// else if (method.getName().equals("close")) {
	// // Handle close method: only close if not within a transaction.
	// doReleaseDatabase(this.target);
	// this.closed = true;
	// return null;
	// }
	// else if (method.getName().equals("isClosed")) {
	// return this.closed;
	// }
	//
	// if (this.closed) {
	// throw new SQLException("Database is already closed");
	// }
	//
	// if (method.getName().equals("getTargetDatabase")) {
	// // Handle getTargetConnection method: return underlying DB.
	// return this.target;
	// }
	//
	// // Invoke method on target DB.
	// try {
	// Object retVal = method.invoke(this.target, args);
	// return retVal;
	// }
	// catch (InvocationTargetException ex) {
	// throw ex.getTargetException();
	// }
	// }
	// }

}
