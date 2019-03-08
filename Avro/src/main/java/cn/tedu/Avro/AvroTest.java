package cn.tedu.Avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import avro.pojo.User;

public class AvroTest {
	@Test
	public void create() {
		User u1=new User();
		u1.setAge(15);
		u1.setUsername("Jessica");
		User u2=new User("Yue",18,"女");
		System.out.println(u1);
		System.out.println(u2);
		//基于U2对象来进行创建，在创建过程中改变新对象的age属性的值
		User u3=User.newBuilder(u2).setAge(17).build();
		System.out.println(u3);
	}
	
	//序列化
	@Test
	public void serial() throws IOException {
		User u1=new User("Helen",15,"女");
		User u2=new User("John",18,"男");
		//创建avro中的序列化流
		DatumWriter<User> dw=new SpecificDatumWriter<>(User.class);
		//创建一个AVRO中的文件流来保存序列化的数据
		DataFileWriter<User> df=new DataFileWriter<>(dw);
		//指定数据的写出位置
		//schema--约束，表示将以无明 按照哪种格式写出 
		//file 
		df.create(u1.getSchema(), new File("a.txt"));
		//序列化对象
		df.append(u1);
		df.append(u2);
		//关流
		df.close();
	}
	
	//反序列化
	@Test
	public void desrial() throws IOException {
		//创建AVRO的反序列化流
		DatumReader<User> dr=new SpecificDatumReader<>(User.class);
		//创建AVRO中的文件流
		DataFileReader<User> df=new DataFileReader<>(new File("a.txt"), dr);
		
		//反序列化
		//AVRO会将要反序列化的所有的对象放入一个迭代器中，允许以迭代器形式来读取
		while(df.hasNext()) {
			User u=df.next();
			System.out.println(u);
		}
		//关流
		df.close();
	}
}
