package com.ravi.sparkspring.poc.job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ravi.sparkspring.poc.beans.Count;
import com.ravi.sparkspring.poc.beans.Word;
import com.ravi.sparkspring.poc.config.ApplicationConfiguration;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

@Component
public class WordCountJob implements Serializable   {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Autowired
    private SparkSession sparkSession;
    
    @Autowired
    ApplicationConfiguration applicationConfiguration;

    public List<Count> count() {
        //String input = "hello world hello hello hello";
        String input = applicationConfiguration.getInputString();
        String[] _words = input.split(" ");
        List<Word> words = Arrays.stream(_words).map(Word::new).collect(Collectors.toList());
        Dataset<Row> dataFrame = sparkSession.createDataFrame(words, Word.class);
        dataFrame.show();
        //StructType structType = dataFrame.schema();

       /* RelationalGroupedDataset groupedDataset = dataFrame.groupBy(col("word"));
        groupedDataset.count().show();
        List<Row> rows = groupedDataset.count().collectAsList();//JavaConversions.asScalaBuffer(words)).count();
        return rows.stream().map(new Function<Row, Count>() {
            @Override
            public Count apply(Row row) {
                return new Count(row.getString(0), row.getLong(1));
            }
        }).collect(Collectors.toList());*/
        
        Count c= new Count("ravi",3);
        		Count c1= new Count("fresh",1);
        				Count c2= new Count("air",1);
        				
        				List<Count> arrlist = new ArrayList<Count>(5);
        				
        				
        arrlist.add(c);
        arrlist.add(c1);
        arrlist.add(c2);
        return arrlist;
        
    }

    public void kafkaCount() throws InterruptedException {
    	
    	JavaStreamingContext jssc = new JavaStreamingContext(applicationConfiguration.javaSparkContext(),Durations.seconds(10));
    	
    	Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    	Map<String, Integer> topics = new HashMap<String, Integer>();
        topics.put("kafkamicroservices", 1);
        Set<String> topicset= new TreeSet<>();
        topicset.add("kafkamicroservices");
        
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
    	
        JavaPairDStream<String, String> input=	 KafkaUtils.createStream(jssc, "localhost:2181","demo",topics );
        System.out.println("inside kafka");
        System.out.println("jkfksdjf"+input.count().toString());
        
        /*JavaPairDStream<String, String> input=	 KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
        		StringDecoder.class, kafkaParams, topicset);*/
        
       // JavaDStream<String> lines=input.map(Tuple2::_2);
        
        JavaDStream<String> lines= input.map(x -> x._2);
        
        lines.print();
        
        System.out.println("inside kafka 888888");
        jssc.start();
        System.out.println("inside kafka  end ");
        jssc.awaitTermination();
    	   // return "KafkaInput";
    }
}
