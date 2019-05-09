package com.apporchid.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class HiveDriver implements Driver {
	  static {
		    try {
		      java.sql.DriverManager.registerDriver(new HiveDriver());
		    } catch (SQLException e) {
		      // TODO Auto-generated catch block
		      e.printStackTrace();
		    }
		  }
	
	org.apache.hive.jdbc.HiveDriver driver;
	public HiveDriver() throws SQLException{
		DriverManager.registerDriver(this);
		driver = new org.apache.hive.jdbc.HiveDriver();
	}
	
	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return driver.acceptsURL(url);
	}

	@Override
	public Connection connect(String url, Properties props) throws SQLException {
		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication","kerberos");
		UserGroupInformation.setConfiguration(conf);
		
		String user = props.getProperty("user");
		String password = props.getProperty("password");
		
		LoginContext lc;
		try {
			lc = kinit(user,password);
			UserGroupInformation.loginUserFromSubject(lc.getSubject());
			Connection con = driver.connect(url, props);
			return con;
		} catch (Exception e) {
			e.printStackTrace();
		} 
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException e) {
			throw new SQLException("Hive Driver not found", e);
		}
		return DriverManager.getConnection(url,props);
	}
	

	public static LoginContext kinit(String username,String password) throws LoginException {
		char[] passwordChar = password.toCharArray();
		LoginContext lc = new LoginContext("HdfsMain", 
			new CallbackHandler() {
				public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
					for(Callback c : callbacks){
						if(c instanceof NameCallback)
							((NameCallback) c).setName(username);
						if(c instanceof PasswordCallback)
							((PasswordCallback) c).setPassword(passwordChar);
					}
				}
		 	}
		);
		lc.login();
		return lc;
	}

	@Override
	public int getMajorVersion() {
		return driver.getMajorVersion();
	}

	@Override
	public int getMinorVersion() {
		return driver.getMinorVersion();
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return driver.getParentLogger();
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return driver.getPropertyInfo(url, info);
	}

	@Override
	public boolean jdbcCompliant() {
		return driver.jdbcCompliant();
	}

}
