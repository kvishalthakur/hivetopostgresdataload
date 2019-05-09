package com.apporchid.hive;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class HiveDriverTest {
	
	public static void main(String[] args) {
		String queryFile = "c:/tmp/data/hive/queryTest.sql";
		String filePath = "c:/tmp/data/hive/queryTest.csv";
		String sql = null;
		
		File f = new File(filePath);
		String extension = "";
		int g = 0;
		while(f.exists()) {
		  int i = f.getName().lastIndexOf('.');
		  if (i > 0) 
		     { extension = f.getName().substring(i+1); }
		   f = new File(f.getParentFile().getPath() + "/" + (f.getName().substring(0,f.getName().lastIndexOf('.')) + g) + "." + extension);
		}
		
		
		
		try {
			sql = new String(Files.readAllBytes(Paths.get(queryFile)));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(sql!=null){
			StringBuffer content = runData(sql);
			writeFile(f, content.toString());
		}else{
			System.err.println("Could not load SQL from file "+ queryFile);
		}
	}
		
	
	public static void writeFile(File file, String content){
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
			bw.write(content);
			System.out.println("Wrote to file "+file.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static StringBuffer runData(String sql) {
		Connection conn = null;
		StringBuffer content = new StringBuffer();
		String url = "jdbc:hive2://hsyplhdps001.amwater.net:2181,hsyplhdps003.amwater.net:2181,hsyplhdps002.amwater.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-hive2";
		String user = "maniy";
		String password = "Bre@k1tUp";
		try{
			conn = loginConnect(url, user, password);
			System.out.println("got connection");		
			System.out.println("Executing query");
			ResultSet rs = conn.createStatement().executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();
			for(int i=1;i<=rsmd.getColumnCount();i++){
				String columnName = rsmd.getColumnName(i).indexOf(".")>0?rsmd.getColumnName(i).substring(rsmd.getColumnName(i).indexOf(".")+1):rsmd.getColumnName(i);
				content.append(columnName+",");
			}
			content.append("\n");
			while (rs.next()) {
				for(int i=1;i<=rsmd.getColumnCount();i++){
					content.append("\""+rs.getString(i)+"\",");
				}
				content.append("\n");
			}
			System.out.println("Completed Load");
		}catch(Exception e){
			e.printStackTrace();
		}
		
		try{
			conn.close();
		}catch(Exception e){
			//ignore
		}
		return content;
	}
	
	private static Connection loginConnect(String url,String user, String password){
		
		try {
			Class.forName("com.apporchid.hive.HiveDriver");
			System.out.println("getting connection");
			Properties props = new Properties();
			props.put("user", user);
			props.put("password", password);
			Connection con = DriverManager.getConnection(url,props);
			System.out.println("got connection");
			return con;
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return null;
	}
}