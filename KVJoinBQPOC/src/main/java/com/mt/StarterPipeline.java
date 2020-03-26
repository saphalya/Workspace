/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mt;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class StarterPipeline {
  /*
   * private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);
  private static final String Player_TABLE = "mt-poc:Examples.Player";
  private static final String Project = "mt-poc";
  private static final String StagingLocation = "gs://my_bucket_8627/VZT/Inbound/";
  private static final String TempLocation = "gs://my_bucket_8627/tmp/BigQueryExtractTemp";
  private static final String sourceFile = "gs://my_bucket_8627/VZT/Inbound/VGT_Source.csv";*/
  private static final String targetFileLocation = "gs://my_bucket_8627/VZT/Outbound/";
  
 
  public static void main(String[] args) throws Exception {
      
	  PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
      Pipeline pipeline = Pipeline.create(options);

     

      List<KV<String, String>> data1 = new ArrayList<>();
      data1.add(KV.of("key1", "Tendulkar"));
      data1.add(KV.of("key1", "Dravid"));
      data1.add(KV.of("key2", "Gilippse"));
      PCollection<KV<String, String>> input1 = pipeline.apply(Create.of(data1));

      List<KV<String, Integer>> data2 = new ArrayList<>();
      data2.add(KV.of("key1", 2));
      data2.add(KV.of("key1", 3));
      data2.add(KV.of("key1", 4));
      data2.add(KV.of("key1", 5));
      data2.add(KV.of("key2", 12));
      
      //data2.add(KV.of("key2", 2));
      PCollection<KV<String,  Integer>> input2 = pipeline.apply(Create.of(data2));

      
      
      final TupleTag<String> tag1 = new TupleTag<>();
      final TupleTag<Integer> tag2 = new TupleTag<>();
   
      
      PCollection<KV<String, CoGbkResult>> join = KeyedPCollectionTuple
    		  									  .of(tag1, input1)
    		  									  .and(tag2, input2)
    		  									  .apply(CoGroupByKey.<String>create());

      PCollection<String> finalResults = join
    		  								.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
          /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@ProcessElement
          public void processElement(ProcessContext processContext) {
             
        	  StringBuilder builder = new StringBuilder();
              KV<String, CoGbkResult> element = processContext.element();
              
              System.out.println("Key:"+element.getKey());
              builder.append(element.getKey()).append(",");
              
              Iterable<String> tag1Val = element.getValue().getAll(tag1);
              System.out.println("tag1Val:"+tag1Val);
              
              for (String val : tag1Val) {
                  builder.append(val).append(",");
              }
              
              Iterable<Integer> tag2Val = element.getValue().getAll(tag2);
              System.out.println("tag2Val:"+tag2Val);
             
              for (Integer val : tag2Val) {
                  builder.append(val).append(",");
              }
              
             // builder.append("]");
              
              System.out.println("Output:"+builder.toString());
              processContext.output(builder.toString());
          }
      }));

     //PAssert.that(finalResults).containsInAnyOrder("key1: [value1,2,]");
      finalResults.
      apply("Write To Text",
      	    TextIO.write().to(targetFileLocation+"join_")
      	                .withSuffix(".csv"));
   
    pipeline.run().waitUntilFinish();
  }
}
