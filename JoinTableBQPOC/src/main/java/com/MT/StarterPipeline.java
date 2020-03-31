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
package com.MT;


import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.services.bigquery.model.TableRow;


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
  private static final String playerTable = "mt-poc:Examples.Player";
  private static final String playerTeamTable = "mt-poc:Examples.PlayerTeam";
  private static final String Project = "mt-poc";
  private static final String StagingLocation = "gs://my_bucket_8627/VZT/Inbound/";
  private static final String TempLocation = "gs://my_bucket_8627/tmp/BigQueryExtractTemp";
  private static final String targetFileLocation = "gs://my_bucket_8627/VZT/Outbound/";
  
  
  
  static class ExtractPlayerDataFn extends DoFn<TableRow, KV<String, String>> {
	  private static final long serialVersionUID = 1L;
	  
	  @ProcessElement
	    public void processElement(ProcessContext c) {
	      TableRow row = c.element();
	      
	      String playerPhone = (String) row.get("phone");
	      String playerName = (String) row.get("name");
	      String playerAge = (String) row.get("age");	      
	      String eventInfo = playerName + ", " + playerAge;
	      c.output(KV.of(playerPhone, eventInfo));	     
	    }
	  }
  
  static class ExtractPlayerTeamInfoFn extends DoFn<TableRow, KV<String, String>> {
	  private static final long serialVersionUID = 1L;
	  
	  @ProcessElement
	    public void processElement(ProcessContext c) {
	      TableRow row = c.element();
	      
	      String playerPhone = (String) row.get("phone");
	      String playerCity = (String) row.get("city");
	      String playerTeam = (String) row.get("team");	      
	      String eventInfo = playerCity + ", " + playerTeam;
	      c.output(KV.of(playerPhone, eventInfo));	     
	    }
	  }
     
  static PCollection<String> joinEvents(PCollection<TableRow> playerTableRow, PCollection<TableRow> playerTeamTableRow) throws Exception {
	  
	  	final TupleTag<String> playerInfoTag = new TupleTag<String>();
	    final TupleTag<String> teamInfoTag = new TupleTag<String>();
	    
	    //Create Key Value Pair
	    PCollection<KV<String, String>> playerInfo = playerTableRow.apply(ParDo.of(new ExtractPlayerDataFn()));
	    PCollection<KV<String, String>> teamInfo = playerTeamTableRow.apply(ParDo.of(new ExtractPlayerTeamInfoFn()));
	    
	    
	    PCollection<KV<String, CoGbkResult>> kvpCollection = KeyedPCollectionTuple.of(playerInfoTag, playerInfo)
				  											.and(teamInfoTag, teamInfo)
				  											.apply(CoGroupByKey.<String>create());
	    
	    PCollection<KV<String, String>> finalResultCollection = kvpCollection.apply(ParDo
	    	    		  													.of(new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
	    	        	private static final long serialVersionUID = 1L;

						@ProcessElement
	    	          public void processElement(ProcessContext c) {
	    	            KV<String, CoGbkResult> e = c.element();
	    	            String playerPhone = e.getKey();
	    	            
	    	            String playerTeam = "none";
	    	            playerTeam = e.getValue().getOnly(teamInfoTag);
	    	           
	    	            for (String eventInfo : c.element().getValue().getAll(playerInfoTag)) {
	    	              // Generate a string that combines information from both collection values
	    	              c.output(KV.of(playerPhone, playerTeam + ", " + eventInfo));
	    	            }
	    	          }
	    	      }));
	    
	    PCollection<String> formattedResults = finalResultCollection.apply(ParDo
	    															.of(new DoFn<KV<String, String>, String>() {
	            	private static final long serialVersionUID = 1L;

					@ProcessElement
	              public void processElement(ProcessContext c) {
	                //String outputstring = "Phone Number: " + c.element().getKey() + ", " + c.element().getValue();
					String outputstring = c.element().getKey() + ", " + c.element().getValue();
	                c.output(outputstring);
	              }
	            }));
	        return formattedResults;
	      
  }
  
  
  //Main Function
  
  public static void main(String[] args) throws Exception {
	  
	  /*
	   * Create Pipeline
	   * With the options provided
	   */
	 DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
	  
	// Create and set your PipelineOptions.
	  options.setProject(Project);
	  options.setStagingLocation(StagingLocation);
	  options.setTempLocation(TempLocation);
	  
	// Create the Pipeline with the specified options.
	Pipeline pipeline = Pipeline.create(options);
	
	//Read Player BQ Table
	PCollection<TableRow> playerTableRow = pipeline.apply(BigQueryIO.readTableRows().from(playerTable));
	//Read PlayerTeam BQ Table
	PCollection<TableRow> playerTeamTableRow = pipeline.apply(BigQueryIO.readTableRows().from(playerTeamTable));
		
	//Create Name Value Pair For Player
	PCollection<String> finalResults = joinEvents(playerTableRow, playerTeamTableRow);
	
	//write to a file
	
	finalResults.apply("Write To Text",
    	    TextIO.write().to(targetFileLocation+"Player_").withSuffix(".csv"));
	
	
	pipeline.run().waitUntilFinish();
  }
}
