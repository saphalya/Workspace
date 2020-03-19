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
package com.MT.BigQueryReadPOC;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

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
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);
  private static final String Names_2014_TABLE = "mt-poc:babynames.name_2014";
  
  static class ExtractEventDataFn extends DoFn<TableRow, String> {
	    @ProcessElement
	      public void processElement(ProcessContext compute) {
	      TableRow row = compute.element();
	      String name = (String) row.get("name");
	      String count = (String) row.get("count");
	      
	      if(count.equals("5")) {

	    	  compute.output(name +", High");	    	  
	      }
	      else {
	    	  compute.output(name + ", low");
	      }
	    }// Function Ends
	  }//Class Ends
 
  public static void main(String[] args) {
  	  
	// Create and set your PipelineOptions.
	  DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
	  //Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

	  // For Cloud execution, set the Cloud Platform project, staging location,
	  // and specify DataflowRunner.
	  options.setProject("mt-poc");
	  options.setStagingLocation("gs://my_bucket_8627");
	  options.setTempLocation("gs://my_bucket_8627/tmp");
	  
	// Create the Pipeline with the specified options.
	Pipeline pipeline = Pipeline.create(options);
	
	
    
    PCollection<TableRow> namesTable =
   	pipeline.apply(BigQueryIO.readTableRows().from(Names_2014_TABLE));
        
    PCollection<String> TypeInfo =
    		namesTable.apply(ParDo.of(new ExtractEventDataFn()));
   
   //Write To A File 
   TypeInfo.
    apply("Write To Text",
    	    TextIO.write().to("gs://my_bucket_8627/VZT/outbound")
    	                .withSuffix(".csv"));

   	//Write To Big Query
   

    pipeline.run().waitUntilFinish();
  }


}
