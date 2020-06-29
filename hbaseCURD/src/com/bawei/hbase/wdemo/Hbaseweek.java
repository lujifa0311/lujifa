package com.bawei.hbase.wdemo;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class Hbaseweek {

	static Configuration config = null;
	private Connection connetion = null;
	private Table table = null;
	
	@Before
	public void init() throws IOException{
		//配置hbase
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "lu07");
		config.set("hbase.zookeeper.proterty.clientPort", "2181");
		//创建工厂链接
		connetion = ConnectionFactory.createConnection(config);
		//链接一个表
		table = connetion.getTable(TableName.valueOf("role"));
	}
	
	//修改角色的信息版本
	@SuppressWarnings("deprecation")
	@Test
	public void creaetTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		//创建管理类
		HBaseAdmin admin = new HBaseAdmin(config);
		//创建描述类
		TableName tableName = TableName.valueOf("lujifa");
		HTableDescriptor descriptor = new HTableDescriptor(tableName);
		
		//创建列族
		HColumnDescriptor famliy = new HColumnDescriptor("info");
		descriptor.addFamily(famliy);
		
		admin.createTable(descriptor);
	}
	
	//添加数据
	@Test
	public void insertData(){
		//创建rowkey
		Put put = new Put(Bytes.toBytes("lu_123"));
		
		put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
	}
	
	//全表扫描
	@Test
	public void scanData() throws IOException{
		//创建scan
		Scan scan = new Scan();
		ResultScanner reScanner = table.getScanner(scan);
		
		for (Result result : reScanner) {
			byte[] name = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
			System.out.println(Bytes.toString(name));
		}
		
	}
	
	//单条数据查询
	@Test
	public void getDate() throws IOException{
		Get get = new Get(Bytes.toBytes("200"));
		Get setMaxVersions = get.setMaxVersions();
		System.out.println(setMaxVersions);
		Result result = table.get(get);
		byte[] name = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
		byte[] s= result.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("002"));
		byte[] n= result.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("001"));
		
		System.out.println("name:"+Bytes.toString(name));
		System.out.println("002"+Bytes.toString(s));
		System.out.println("001"+Bytes.toString(n));
		
	}

    //添加新的测试
	public void getTest(){
        Get get = new Get(Bytes.toBytes("200"));
		Get setMaxVersions = get.setMaxVersions();
		System.out.println(setMaxVersions);
		Result result = table.get(get);    
	}
	
}
