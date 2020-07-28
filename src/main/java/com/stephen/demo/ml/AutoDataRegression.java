package com.stephen.demo.ml;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;



import org.apache.log4j.Level;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.stephen.demo.spark.SparkBuilder;
import com.stephen.demo.spark.SparkHelper;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.linalg.Vectors;

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;

import static org.apache.spark.sql.functions.*;

public class AutoDataRegression implements Serializable {
	
	/**
	 * Auto generated UID 
	 */
	private static final long serialVersionUID = 1L;

	private static final SparkSession session = SparkBuilder.createSparkSession();
	
	private static String path = "src\\main\\resources\\data\\auto-data.csv";
	
	public static void main(String[] args) {
		SparkHelper.setLoggerLevel(Level.ERROR);
		AutoDataRegression ad = new AutoDataRegression();
		
		JavaRDD<String> rdd = SparkHelper.LoadCSVData(path);
		ad.mlDecisionTreePrediction(rdd);
		//ad.findMostExpensiveCar(rdd);
	}
	
	
	/**
	 * 	MAKE,FUELTYPE,ASPIRE,DOORS,BODY,DRIVE,CYLINDERS,HP,RPM,MPG-CITY,MPG-HWY,PRICE
	 *	subaru,gas,std,two,hatchback,fwd,four,69,4900,31,36,5118
	 * @return
	 */
	
	private StructType getFullDataSchema() {
		return DataTypes
			.createStructType(new StructField[] {
					DataTypes.createStructField("MAKE", DataTypes.StringType, false),
					DataTypes.createStructField("FUELTYPE", DataTypes.StringType, false),
					DataTypes.createStructField("ASPIRE", DataTypes.StringType, false),
					DataTypes.createStructField("DOORS", DataTypes.StringType, false),
					DataTypes.createStructField("BODY", DataTypes.StringType, false),
					DataTypes.createStructField("DRIVER", DataTypes.StringType, false),
					DataTypes.createStructField("CYCLINDERS", DataTypes.StringType, false),
					DataTypes.createStructField("HP", DataTypes.IntegerType, false),
					DataTypes.createStructField("RPM", DataTypes.IntegerType, false),
					DataTypes.createStructField("MPG-CITY", DataTypes.DoubleType, false),
					DataTypes.createStructField("MPG-HWY", DataTypes.DoubleType, false),
					DataTypes.createStructField("PRICE", DataTypes.DoubleType, false)
		});	
	}
	
	private StructType getDataSchema() {
		return DataTypes
			.createStructType(new StructField[] {
					DataTypes.createStructField("HP", DataTypes.DoubleType, false),
					DataTypes.createStructField("RPM", DataTypes.DoubleType, false),
					DataTypes.createStructField("MPG-CITY", DataTypes.DoubleType, false),
					DataTypes.createStructField("MPG-HWY", DataTypes.DoubleType, false),
					DataTypes.createStructField("PRICE", DataTypes.DoubleType, false),
					DataTypes.createStructField("MAKE", DataTypes.StringType, false)
		});	
	}
	
	private JavaRDD<Row> createCarRDD(JavaRDD<String> autoRdd) {
		String header = autoRdd.first();
		JavaRDD<String> rdd = autoRdd.filter(x->!x.equals(header));
		
		//List<String> list1 = rdd.take(5);		
		//for(String s:list1) {
		//	System.out.println("***********" + s);
		//}
		
		JavaRDD<Row> rows = rdd.map(x -> {
			List<String> list = Arrays.asList(x.split(","));
			Row row = RowFactory.create(
					list.get(0),   //Make
					list.get(1),   //FuelType
					list.get(2),   //ASPIRE
					list.get(3),   //DOORS
					list.get(4),   //BODY
					list.get(5),   //DRIVER
					list.get(6),   //CYCLINDERS
					Double.valueOf(list.get(7)),   //HP
					Double.valueOf(list.get(8)),   //RPM
					Double.valueOf(list.get(9)),   //MPG-CITY
					Double.valueOf(list.get(10)),   //MPG-HWY
					Double.valueOf(list.get(11))   //PRICE
				);
			return row;
		});
		return rows;
	}

	private JavaRDD<Row> createMLRDD(JavaRDD<String> autoRdd) {
		String header = autoRdd.first();
		JavaRDD<String> rdd = autoRdd.filter(x->!x.equals(header));
				
		JavaRDD<Row> rows = rdd.map(x -> {
			List<String> list = Arrays.asList(x.split(","));
			Row row = RowFactory.create(
					Double.valueOf(list.get(7)),   //HP
					Double.valueOf(list.get(8)),   //RPM
					Double.valueOf(list.get(9)),   //MPG-CITY
					Double.valueOf(list.get(10)),   //MPG-HWY
					Double.valueOf(list.get(11)),   //PRICE
					list.get(0)   //Make
				);
			return row;
		});
		return rows;
	}
		
	public Car findMostExpensiveCar(JavaRDD<String> autoRdd) {
		JavaRDD<Row> cars = createCarRDD(autoRdd);
		
		/**
		rows.reduce(new Function2<Row, Row, Double>() {
			@Override
			public Double call(Row v1, Row v2) throws Exception {
				if (x.getDouble(11)>y.getDouble(11)) return x.getDouble(11);
				else return y.getDouble(11)
				}
			
		}); **/
		
		Row result = cars.reduce((x, y) -> {
			if (x.getDouble(11)>y.getDouble(11)) return x;
			else return y;
		});
		
		return createCar(result);
	}
	
	/**
	 * Create Car entity from Spark Row
	 * @param row
	 * @return
	 */
	private Car createCar(Row row) {
		Car car = new Car();
		car.setMaker(row.getString(0));
		car.setFuelType(row.getString(1));
		car.setAspire(row.getString(2));
		car.setDoors(row.getString(3));
		car.setBody(row.getString(4));
		car.setDriver(row.getString(5));
		car.setCyclinders(row.getString(6));
		car.setHP(Integer.valueOf(row.getInt(7)));
		car.setRPM(Integer.valueOf(row.getInt(8)));
		car.setMPGCity(Double.valueOf(row.getDouble(9)));
		car.setMPGHighWay(Double.valueOf(row.getDouble(10)));
		car.setPrice(Double.valueOf(row.getDouble(11)));
		
		return car;
	}

	/**
	 * Sample of decision tree prediction
	 * @param autoRdd
	 */
	public void mlDecisionTreePrediction(JavaRDD<String> autoRdd) {
		
		JavaRDD<Row> cars = createMLRDD(autoRdd);
		
		Dataset<Row> dsCar = session.createDataFrame(cars, getDataSchema());
		
		//dsCar.printSchema();
		
		//correlation analysis
		for (StructField field : getDataSchema().fields()) {
			if (!field.dataType().equals(DataTypes.StringType)) {
				System.out.println("Correlation between HP and " + field.name()
			 	+ " = " + dsCar.stat().corr("HP", field.name()) );
			}
		}
		
		StringIndexer indexer =  new StringIndexer()
				.setInputCol("MAKE")
				.setOutputCol("IDX_MAKE");
		
		StringIndexerModel siModel = indexer.fit(dsCar);
		Dataset<Row> indexCar = siModel.transform(dsCar);
		
		indexCar.groupBy(col("MAKE"), col("IDX_MAKE")).count().show(10);
		
		//Create LablePoint		
		JavaRDD<Row> rddCar = indexCar.toJavaRDD().repartition(2);		
		
		JavaRDD<LabeledPoint> lpCar = rddCar.map(new Function<Row, LabeledPoint>() {
			@Override
			public LabeledPoint call(Row v1) throws Exception {
				// TODO Auto-generated method stub
				LabeledPoint lp = new LabeledPoint(v1.getDouble(6),
						Vectors.dense(
								v1.getDouble(0),
								v1.getDouble(1),
								//v1.getDouble(2),
								v1.getDouble(3),
								v1.getDouble(4)
								)
						
						);
				
				return lp;
			}
			
		});
				
		
		Dataset<Row> dsCar2 = session.createDataFrame(lpCar, LabeledPoint.class);
		
		Dataset<Row>[] splits = dsCar2.randomSplit(new double[] {0.90, 0.10});
		Dataset<Row> training = splits[0];
		Dataset<Row> test = splits[1];
		
		//Decision Tree ML
		DecisionTreeClassifier decisionTree = new DecisionTreeClassifier();
		decisionTree.setLabelCol("label");
		decisionTree.setFeaturesCol("features");
		
		DecisionTreeClassificationModel model = decisionTree.fit(training);
		
		IndexToString labelIndex = new IndexToString()
				.setInputCol("label")
				.setOutputCol("labelMake")
				.setLabels(siModel.labels());
		
		IndexToString convertPrediction =  new IndexToString()
				.setInputCol("prediction")
				.setOutputCol("predictionResult")
				.setLabels(siModel.labels());
				
		//Predication model
		Dataset<Row> pred = model.transform(test);
		
		Dataset<Row> prediction = convertPrediction.transform(
					labelIndex.transform(pred)
				);

		//check predicion result
		prediction.select("labelMake", "predictionResult", "features").show(5);
		
		//generate confusion matrix
		prediction.groupBy(col("labelMake"), col("predictionResult")).count().show();
		
		//evaluate the accuracy
		MulticlassClassificationEvaluator evaluator =  new MulticlassClassificationEvaluator()
				.setLabelCol("label")
				.setPredictionCol("prediction")
				.setMetricName("accuracy");
		double dAccuracy = evaluator.evaluate(prediction);
		
		System.out.println("The accuracy of prediction : " + dAccuracy);
	}

}
