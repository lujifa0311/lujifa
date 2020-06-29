package com.bawei.hbase.demo2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HBase {

	static Configuration config = null;
	private Connection connection = null;
	private Table table = null;
	
	@Before
	public void init() throws IOException{
		//配置hbase
		config = HBaseConfiguration.create();
		//创建连接
		config.set("hbase.zookeeper.quorum", "lu07");
		config.set("hbase.zookeeper.proterty.clientPort", "2181");
		
		//创建工厂链接
		connection = ConnectionFactory.createConnection();
		
		//链接一个表内
		table = connection.getTable(TableName.valueOf("luji"));
		
	}
	
	//创建一个表
	@Test
	public void createTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		//创建管理类
		HBaseAdmin admin = new HBaseAdmin(config);
		
		//自定义表名
		TableName tableName = TableName.valueOf("luji");
		//创建描述类
		HTableDescriptor descriptor = new HTableDescriptor(tableName);
		
		//创建列族
		HColumnDescriptor famliy = new HColumnDescriptor(Bytes.toBytes("info"));
		descriptor.addFamily(famliy);
		
		//创建表
		admin.createTable(descriptor);	
	}
	
	//删除表
	@Test
	public void deleteTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		//创建管理类
		HBaseAdmin admin = new HBaseAdmin(config);
		admin.disableTable("luji");
		admin.deleteTable("luji");
		admin.close();
		
	}
	
	//向表中添加数据
	@Test
	public void insertData() throws IOException{
		//创建put
		ArrayList<Put> list = new ArrayList<Put>();
		Put p1 = new  Put(Bytes.toBytes("lu_01"));
		p1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
		p1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes("nan"));
		p1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(18));
		
		Put p2 = new  Put(Bytes.toBytes("lu_02"));
		p2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
		p2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes("nan"));
		p2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(23));
		
		list.add(p1);
		list.add(p2);
		
		table.put(list);
	}
	
	//删除数据
	@Test
	public void deleteData() throws IOException{
		Delete delete = new Delete(Bytes.toBytes("lu_01"));
		delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("nmae"));
		
		table.delete(delete);
	}
	
	//单条查询
	@Test
	public void getData() throws IOException{
		Get get = new Get(Bytes.toBytes("lu_01"));
		Result result = table.get(get);
		byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
		byte[] sex = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex"));
		byte[] age = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
		System.out.println(Bytes.toString(name));
		System.out.println(Bytes.toString(sex));
		System.out.println(Bytes.toString(age));
	}
	
	//扫描全表
	@Test
	public void scanGet() throws IOException{
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		
		for (Result result : scanner) {
			byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
			byte[] sex = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex"));
			byte[] age = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
			System.out.println(Bytes.toString(name));
			System.out.println(Bytes.toString(sex));
			System.out.println(Bytes.toString(age));
		}
	}
	
	//列值过滤器
	@Test
	public void scanFiler1() throws IOException{
		Scan scan = new Scan();
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareOp.EQUAL, Bytes.toBytes("zhangsan"));
		//设置过滤器
		Scan setFilter = scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(setFilter);
		for (Result result : scanner) {
			byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
			byte[] sex = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex"));
			byte[] age = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
			System.out.println(Bytes.toString(name));
			System.out.println(Bytes.toString(sex));
			System.out.println(Bytes.toString(age));
		}
	}
	
	//rowKey过滤器
	@Test
	public void scanFiler2() throws IOException{
		Scan scan = new Scan();
		//创建过滤器
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^lu"));
		//设置过滤器
		Scan setFilter = scan.setFilter(rowFilter);
		ResultScanner scanner = table.getScanner(setFilter);
		for (Result result : scanner) {
			byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
			byte[] sex = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex"));
			byte[] age = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
			System.out.println(Bytes.toString(name));
			System.out.println(Bytes.toString(sex));
			System.out.println(Bytes.toString(age));
		}
	}
	
	//列前缀过滤器
	@Test
	public void scanFiler3() throws IOException{
		Scan scan = new  Scan();
		//设置过滤器
		ColumnPrefixFilter prefixFilter = new ColumnPrefixFilter(Bytes.toBytes("na"));
		Scan setFilter = scan.setFilter(prefixFilter);
		ResultScanner scanner = table.getScanner(setFilter);
		for (Result result : scanner) {
			byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
			byte[] sex = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex"));
			byte[] age = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
			System.out.println(Bytes.toString(name));
			System.out.println(Bytes.toString(sex));
			System.out.println(Bytes.toString(age));
		}
	}
	
	//组合过滤器
	@Test 
	public void scanFilelist() throws IOException{
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^lu"));
		SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareOp.EQUAL, Bytes.toBytes("wangwu"));
		filterList.addFilter(columnValueFilter);
		filterList.addFilter(rowFilter);
		Scan scan = new Scan();
		Scan setFilter = scan.setFilter(filterList);
		ResultScanner scanner = table.getScanner(setFilter);
		for (Result result : scanner) {
			byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
			System.out.println(Bytes.toString(name));
		}
	}
	

}
