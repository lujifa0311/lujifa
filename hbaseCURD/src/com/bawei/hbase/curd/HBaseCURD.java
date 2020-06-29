package com.bawei.hbase.curd;

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
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HBaseCURD {

	static Configuration config=null;
	private Connection connection = null;
	private Table table = null;
	
	@Before
	//初始化hbase
	public void init() throws IOException{
		//1.配置hbase
		config = HBaseConfiguration.create();
		//2.指定zk的地址  注意win上的 host映射
		config.set("hbase.zookeeper.quorum", "192.168.12.150");
		config.set("hbase.zookeeper.proterty.clientPort", "2181");
		//3.通过工厂模式创建hbase的链接
	connection= ConnectionFactory.createConnection(config);
	
	//4.连接一个表
	table=connection.getTable(TableName.valueOf("max1703"));
		
	}
	//创建一个表  max1703
		@SuppressWarnings("deprecation")
		@Test
		public void createTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
			//1.创建表的管理类
			HBaseAdmin admin=new HBaseAdmin(config);
			//自定义表名
			TableName tableName = TableName.valueOf("max1703");
			//2.创建表的描述类
			HTableDescriptor desc = new HTableDescriptor(tableName);
			
			//3.创建列族的描述类
			HColumnDescriptor family1 = new HColumnDescriptor("info1");
			
			desc.addFamily(family1);
			HColumnDescriptor family2 = new HColumnDescriptor("info2");
			desc.addFamily(family2);
			//创建表
			admin.createTable(desc);			
			
		}
		
		//删除表
		@SuppressWarnings("deprecation")
		@Test
		public void deleteTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
			//表管理类
			HBaseAdmin admin = new HBaseAdmin(config);
			//把表禁用
			admin.disableTable("t1");
			admin.deleteTable("t1");
			admin.close();
		}
		
		@SuppressWarnings("deprecation")
		@Test
		//hbase表中插入数据
		public void insertData() throws IOException{
			//rowkey
			Put put = new Put(Bytes.toBytes("mc_124"));
			//添加列族数据
			put.add(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
			put.add(Bytes.toBytes("info1"), Bytes.toBytes("sex"), Bytes.toBytes("nan"));
			put.add(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes(20));
			put.add(Bytes.toBytes("info1"), Bytes.toBytes("addr"), Bytes.toBytes("beijing"));
			
			table.put(put);
		}
		
		//删除hbase中的数据
		@Test
		public void deleteDate() throws IOException{
			
			Delete delete = new Delete(Bytes.toBytes("mc_123"));
			delete.addColumns(Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
			table.delete(delete);
		}
		
		//单个查询数据
		@Test
		public void queryData() throws IOException{
			Get get = new Get(Bytes.toBytes("mc_123"));
			Result result = table.get(get);
			byte[] name = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"));
			byte[] age = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("age"));
			System.out.println(Bytes.toString(name));
			System.out.println(Bytes.toInt(age));
		}
		
		//全表扫描
		@Test
		public void scanData() throws IOException{
			
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(9));
			scan.setStopRow(Bytes.toBytes("z"));
			ResultScanner scanner = table.getScanner(scan);
			
			for (Result result : scanner) {
				byte[] name = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"));
				System.out.println(Bytes.toString(name));
				System.out.println(Bytes.toString(result.getRow()));
			}
		}
		
		//过滤器
		//列值过滤器.......把age是18的人的name取出来
		@Test
		public void scanDataByFilter1() throws IOException{
			
			//先创建过滤器
			SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info1"), Bytes.toBytes("name"), CompareOp.EQUAL, Bytes.toBytes("zhangsan"));
			
			//设置scan
			Scan scan = new Scan();
			
			//设置过滤器
			Scan setFilter = scan.setFilter(filter);
			
			//全表扫描
			ResultScanner scanner = table.getScanner(setFilter);
			
			for (Result result : scanner) {
				byte[] name = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"));
				
				System.out.println(Bytes.toString(name));
				System.out.println(Bytes.toString(result.getRow()));
			}
		}
		
		//rowkey过滤器
		@Test
		public void scanDataFiler2() throws IOException{
			
			//创建过滤器
			RowFilter rowFilter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator("^mc_"));
			//设置scan
			Scan scan = new Scan();
			//设置过滤器
			Scan setFilter = scan.setFilter(rowFilter);
			//全表扫描
			ResultScanner scanner = table.getScanner(setFilter);
			
			for (Result result : scanner) {
				byte[] name = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"));
				System.out.println(Bytes.toString(name));
				System.out.println(Bytes.toString(result.getRow()));
				
			}
			
		}
		
		//列前缀过滤器
		@Test
		public void scanDataFiler() throws IOException{
			//创建过滤器
			ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("..."));
			//创建 scan
			Scan scan = new  Scan();
			//设置过滤器
			Scan setFilter = scan.setFilter(columnPrefixFilter);
			//扫描全表
			ResultScanner scanner = table.getScanner(setFilter);
			
			for (Result result : scanner) {
				byte[] name = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"));
				
				System.out.println(Bytes.toString(name));
				System.out.println(Bytes.toString(result.getRow()));
			}
			
		}
		
}
