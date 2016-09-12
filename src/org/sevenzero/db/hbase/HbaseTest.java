package org.sevenzero.db.hbase;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;



/**
 * 
 * @author lb
 * 
 * @version 1.0.1
 * 
 * @Description
 *
 * @date 2016年8月23日
 * 
 *
 */
public class HbaseTest {

	private static final Logger log = Logger.getLogger(HbaseTest.class.getSimpleName());
	
	// 声明静态配置
    static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
//        configuration.set("hbase.master", "hdfs://192.168.0.31:60000"); 
    }
	
	public static void main(String[] args) {
		
		log.info(System.getenv("JAVA_HOME"));

		String tableName = "test_from_java";
		createTable(tableName);
		
		
		tableName = "scores";
		String[] family = { "grade", "course"};
		createTable(tableName, family);
		
		insert(tableName, "zkb", family[0], "", "5");
		insert(tableName, "zkb", family[1], "", "95");
		insert(tableName, "zkb", family[1], "math", "75");
		insert(tableName, "zkb", family[1], "art", "85");
		
		insert(tableName, "baoniu", family[0], "", "4");
		insert(tableName, "baoniu", family[1], "math", "84");
		
		getAll(tableName);
		
		getOne(tableName, "zkb");
		
//		delete(tableName, "baoniu");
		
//		deleteTable(tableName);
		
	}
	
	/**
	 * 删除
	 * @param tableName
	 * @param rowKey
	 */
	public static void delete(String tableName, String rowKey) {
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf(tableName));
			
			Delete del = new Delete(rowKey.getBytes());
			table.delete(del);
			
			
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			closeTable(table);
			closeConnection(connection);
		}
	}
	
	/**
	 * 获取一条数据
	 * @param tableName
	 * @param rowKey
	 */
	public static void getOne(String tableName, String rowKey) {
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf(tableName));
			
			Get get = new Get(rowKey.getBytes());
			Result rs = table.get(get);
			for (Cell c : rs.rawCells()) {
				System.out.println( new String( CellUtil.cloneRow(c)) + " " + 
						new String(CellUtil.cloneFamily(c)) + ":" + 
						new String(CellUtil.cloneQualifier(c)) + " " +
						c.getTimestamp() + " " +
						new String(CellUtil.cloneValue(c)) );
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			closeTable(table);
			closeConnection(connection);
		}
	}
	
	/**
	 * 获取所有数据
	 * @param tableName
	 */
	public static void getAll(String tableName) {
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
//				for (KeyValue kv : r.raw()) {
//					System.out.println(new String(kv.getRow()) + " " + 
//							new String(kv.getFamily()) + ":" + 
//							new String(kv.getQualifier()) + " " + 
//							kv.getTimestamp() + " " + 
//							new String(kv.getValue()));
//				}
				for (Cell c : r.rawCells()) {
					System.out.println( new String( CellUtil.cloneRow(c)) + " " + 
							new String(CellUtil.cloneFamily(c)) + ":" + 
							new String(CellUtil.cloneQualifier(c)) + " " +
							c.getTimestamp() + " " +
							new String(CellUtil.cloneValue(c)) );
				}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			closeTable(table);
			closeConnection(connection);
		}
	}
	
	/**
	 * 插入
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public static void insert(String tableName, String rowKey, String family, String qualifier, String value) {
		Connection connection = null;
		Table table = null;
		try {
//			HTable table = new HTable(conf, tableName);
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf(tableName));
			
//			String rowKey = "";
//			String family = "";
//			String qualifier = "";
//			String value = "";
			
			Put put = new Put(Bytes.toBytes(rowKey));
//			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			
			table.put(put);
			
			log.info("添加数据成功");
			table.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			closeTable(table);
			closeConnection(connection);
		}
		
	}
	
	/**
	 * 删除表
	 * @param tableName
	 */
	public static void deleteTable(String tableName) {
		Connection connection = null;
		Admin admin = null;
		try {
//			HBaseAdmin admin = new HBaseAdmin(conf);
			
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
			
			if (admin.tableExists(TableName.valueOf(tableName))) {
				admin.disableTable(TableName.valueOf(tableName));
				admin.deleteTable(TableName.valueOf(tableName));
				
				log.info("delete table " + tableName + " ok.");
			}
		}
		catch (MasterNotRunningException e) {
			e.printStackTrace();
		}
		catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			closeAdmin(admin);
			closeConnection(connection);
		}
	}
	
	/**
	 * 创建表
	 */
	public static void createTable(String tableName, String[] family) {
		Connection connection = null;
		Admin admin = null;
		try {
//			HBaseAdmin admin = new HBaseAdmin(conf);
			
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
			
			if (!admin.tableExists(TableName.valueOf(tableName))) {
//				HTableDescriptor tableDesc = new HTableDescriptor(tableName);
				HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
				for (String s : family) {
				    HColumnDescriptor cf = new HColumnDescriptor(s);  
				    tableDesc.addFamily(cf);  
				}
				admin.createTable(tableDesc);
			    
			    log.info("创建成功");
			}
			else {
				log.info(tableName + ", 已存在");
			}
		}
		catch (MasterNotRunningException e) {
			e.printStackTrace();
		}
		catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			closeAdmin(admin);
			closeConnection(connection);
		}
	}
	
	/**
	 * 关闭 Connection
	 * @param connection
	 */
	public static void closeConnection(Connection connection) {
		if (null != connection) {
			try {
				connection.close();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 关闭 Admin
	 * @param admin
	 */
	public static void closeAdmin(Admin admin) {
		if (null != admin) {
			try {
				admin.close();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 关闭 Table
	 * @param table
	 */
	public static void closeTable(Table table) {
		if (null != table) {
			try {
				table.close();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 创建表
	 */
	public static void createTable(String tableName) {
		Connection connection = null;
		Admin admin = null;
		try {
//			HBaseAdmin admin = new HBaseAdmin(conf);
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();
			
//			String tableName = "test_from_java";
			if (!admin.tableExists(TableName.valueOf(tableName))) {
//				HTableDescriptor tableDesc = new HTableDescriptor(tableName);
				HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			    HColumnDescriptor cf=new HColumnDescriptor("cf");  
			    tableDesc.addFamily(cf);  
			    admin.createTable(tableDesc); 
			    
			    log.info("创建成功");
			}
			else {
				log.info(tableName + ", 已存在");
			}
			
		}
		catch (MasterNotRunningException e) {
			e.printStackTrace();
		}
		catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			closeAdmin(admin);
			closeConnection(connection);
		}
	}

}
