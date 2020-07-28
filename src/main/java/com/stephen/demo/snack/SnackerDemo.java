package com.stephen.demo.snack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import com.stephen.demo.spark.SparkBuilder;


/**
 * 
 * Apache Spark sample
 * @author qwang
 *
 */
public class SnackerDemo {

	private static final SparkSession spark = SparkBuilder.createSparkSession();

	
	public void main(String[] args) throws AnalysisException, IOException {
		
		//Snacker & products data from APIs
		//System.out.println(System.getProperty("user.dir"));
		//String pathSnackers = "https://s3.amazonaws.com/misc-file-snack/MOCK_SNACKER_DATA.json";
		//demo.generateUrlContext("https://ca.desknibbles.com/products.json?limit=100", file);
		
		String filesnackers = "src\\main\\resources\\data\\snacks.json";
		String fileProducts = "src\\main\\resources\\data\\products.json";
        
		
		SnackerDemo demo = new SnackerDemo();
		
		demo.listAllFaveStacks(demo.readDatasetByJSon(filesnackers), demo.getVariants(demo.loadProducts(fileProducts)));
		
		//demo.listAllFaveStacks(demo.readDatasetByJSon(pathSnackers));
		//Dataset<Row> products = demo.loadProducts(fileProducts);
		//demo.processProducts(demo.readDatasetByJSon(fileProducts));
		
	}
	
	public Dataset<Row> readDatasetByJSon(String path) {
		return spark.read().json(path);
	}
	
	private Dataset<Row> loadProducts(String path) {
		//Dataset<Product> products = spark.read().json(path).as(Encoders.bean(Product.class)).toDF();
		return spark.read().json(path).as((Encoders.bean(Products.class))).toDF();
	}

	
	private void listAllFaveStacks(Dataset<Row> data, Dataset<Row> variants) throws AnalysisException {
		Dataset<Row> snacks = data.filter(data.col("fave_snack").isNotNull());
		
		snacks.createTempView("Snackers");

		JavaRDD<Row> rddSnacks = spark.sql("Select id, email, fave_snack from Snackers").javaRDD();
		 
		int emailCount = (int) rddSnacks.map(x->x.get(1)).distinct().count();

		JavaRDD<Row> newRdd = rddSnacks.flatMap(x -> {
			List<Row> rows = new ArrayList<Row>();
			Row row;
			String email = x.getAs("email").toString();
			String[] snackArray = x.getAs("fave_snack").toString().split(",");
			for (int i= 0; i<snackArray.length; i++) {
				row = RowFactory.create(email, snackArray[i].trim());
				rows.add(row);
			}
			
			return rows.iterator();
		});
				
		StructType schemaSnack = DataTypes
			.createStructType(new StructField[] {
					DataTypes.createStructField("Snack", DataTypes.StringType, false),
					DataTypes.createStructField("Num", DataTypes.IntegerType, false)
		});		
		
		JavaRDD<Row> rddCount = newRdd.map(x -> {
			String snack = x.getString(1);
			Integer count  = 1;
			Row row = RowFactory.create(String.valueOf(snack), Integer.valueOf(count));
			return row;
		});
		
		
		Dataset<Row> ds = spark.createDataFrame(rddCount, schemaSnack);
		
		ds.createTempView("Snacks");
		
		ds.printSchema();
		
		Dataset<Row> snackCounts = ds.sqlContext().sql("select Snack, sum(Num) as Nums from Snacks group by Snack").toDF();
		
		Row[] rowCount = (Row[]) snackCounts.orderBy(desc("Nums")).take(1);
		
		int snackCount = (int) newRdd.map(x->x.get(1)).distinct().count();
		
		System.out.println("Output: {total emails with Fave_Snack:" + emailCount + ", total different snacks in Fave_Snack:" + snackCount 
				+ ", popular snack:" + rowCount[0].get(0) + ", snack count:" + rowCount[0].get(1) + " }");	
		//Output: {total emails with Fave_Snack:499, total different snacks in Fave_Snack:626, popular snack:Okuneva, snack count:5 }

	}
	
	/**
	 * generate the Variant dataset from Products JSON
	 * @param prods
	 * @return
	 * @throws AnalysisException
	 */
	private Dataset<Row> getVariants(Dataset<Row> prods) throws AnalysisException {
		Dataset<Products> products = prods.as(Encoders.bean(Products.class));
		
		products.createTempView("Product");
		
		Dataset<Row> vars = products.sqlContext().sql("Select Product.products.variants from Product").toDF();
		
		Dataset<Row> variants = vars.select(explode(col("variants")));
		variants.createTempView("Variant");
		
		Dataset<Row> dsVariant = variants.sqlContext().sql("Select Variant.col.id, Variant.col.title, Variant.col.price, Variant.col.product_id from Variant");
		
		dsVariant.printSchema();
		
		return dsVariant.toDF();
	}
	
	/**
	 * Query products JSON data
	 * @param prods
	 * @throws AnalysisException
	 */
	private void processProducts(Dataset<Row> prods) throws AnalysisException {		
		Dataset<Row> dsProd = prods.select(explode(col("products"))).toDF();
		//dsProd.schema().printTreeString();
		prods.createTempView("Product");

		Dataset<Row> ds = prods.toDF().sqlContext().sql("Select Product.products.id, Product.products.created_at, Product.products.vendor, Product.products.product_type  from Product");
		ds.show(5);

		dsProd.createTempView("Prod");
		Dataset<Row> ds2 = prods.toDF().sqlContext().sql("Select Prod.col.id, Prod.col.created_at, Prod.col.vendor, Prod.col.product_type  from Prod");
		ds2.show(5);
	
	}
	
}
