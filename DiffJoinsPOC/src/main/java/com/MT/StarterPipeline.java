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



import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.api.services.bigquery.model.TableRow;

/**

 */
public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);  
  private static final String playerTable = "mt-poc:Examples.Player";
  private static final String paymentCategory = "mt-poc:Examples.Paymentcategory";
  private static final String Project = "mt-poc";
  private static final String StagingLocation = "gs://my_bucket_8627/VZT/Inbound/";
  private static final String TempLocation = "gs://my_bucket_8627/tmp/BigQueryExtractTemp";
  private static final String targetFileLocation = "gs://my_bucket_8627/VZT/Outbound/";  
  
  static class ExtractPlayerDataFn extends DoFn<TableRow, KV<String, String>> {
	  private static final long serialVersionUID = 1L;
	  
	  @ProcessElement
	    public void processElement(ProcessContext c) {
	      TableRow row = c.element();
	      
	      String playerCat = (String) row.get("Category");
	      String playerPhone = (String) row.get("phone");
	      String playerName = (String) row.get("name");
	      String playerAge = (String) row.get("age");	      
	      String eventInfo = playerName + ", " + playerAge + ", "+ playerPhone;
	      c.output(KV.of(playerCat, eventInfo));	     
	    }
	  }
  
  static class ExtractPlayerTeamInfoFn extends DoFn<TableRow, KV<String, String>> {
	  private static final long serialVersionUID = 1L;
	  
	  @ProcessElement
	    public void processElement(ProcessContext c) {
	      TableRow row = c.element();
	      
	      String category = (String) row.get("Category");
	      String playerCity = (String) row.get("Basepayment");
	      String eventInfo = playerCity;
	      c.output(KV.of(category, eventInfo));	     
	    }
	  }
  
  static class extractResultString extends DoFn<KV<String, KV<String, String>>,String> {
	  private static final long serialVersionUID = 1L;
	  
	  @ProcessElement
	    public void processElement(ProcessContext c) {
		      KV<String,KV<String, String>> info = c.element();
		      String keyFinal = info.getKey();
		      KV<String, String> valueInfo = info.getValue();
		      
		      String valueFinal = valueInfo.getValue();
		      String interimKey = valueInfo.getKey();
		      System.out.println(keyFinal+", "+interimKey+", "+valueFinal);
		      
		      c.output(keyFinal+", "+interimKey+", "+valueFinal);
	      
	      }
	  }
  
//Inner Join
  static PCollection<String> innerJoin(PCollection<TableRow> playerTableRow, PCollection<TableRow> paymentCategoryTableRow) throws Exception {
	  
	  PCollection<KV<String, String>> playerInfo = playerTableRow.apply(ParDo.of(new ExtractPlayerDataFn()));
	  PCollection<KV<String, String>> paymentCategoryInfo = paymentCategoryTableRow.apply(ParDo.of(new ExtractPlayerTeamInfoFn()));
	  
	  PCollection<KV<String, KV<String, String>>> joinInCollection = Join.innerJoin(paymentCategoryInfo,playerInfo);
	  PCollection<String> finalFormattedResult = joinInCollection.apply(ParDo.of(new extractResultString()));
	  
	  return finalFormattedResult;
  }
 
//Outer Join
 static PCollection<String> outerJoin(PCollection<TableRow> playerTableRow, PCollection<TableRow> paymentCategoryTableRow) throws Exception {
	  
	  PCollection<KV<String, String>> playerInfo = playerTableRow.apply(ParDo.of(new ExtractPlayerDataFn()));
	  PCollection<KV<String, String>> paymentCategoryInfo = paymentCategoryTableRow.apply(ParDo.of(new ExtractPlayerTeamInfoFn()));
	  
	  PCollection<KV<String, KV<String, String>>> joinOutCollection = Join.fullOuterJoin(paymentCategoryInfo,playerInfo,"", "");
	  PCollection<String> finalFormattedResult = joinOutCollection.apply(ParDo.of(new extractResultString()));
	  
	  return finalFormattedResult;
  }
 
 //Left Join
 static PCollection<String> lftJoin(PCollection<TableRow> playerTableRow, PCollection<TableRow> paymentCategoryTableRow) throws Exception {
	  
	  PCollection<KV<String, String>> playerInfo = playerTableRow.apply(ParDo.of(new ExtractPlayerDataFn()));
	  PCollection<KV<String, String>> paymentCategoryInfo = paymentCategoryTableRow.apply(ParDo.of(new ExtractPlayerTeamInfoFn()));
	  
	  PCollection<KV<String, KV<String, String>>> joinOutCollection = Join.leftOuterJoin(paymentCategoryInfo,playerInfo,"");
	  PCollection<String> finalFormattedResult = joinOutCollection.apply(ParDo.of(new extractResultString()));
	  
	  return finalFormattedResult;
 }
 
 //Right Join
 static PCollection<String> rghtJoin(PCollection<TableRow> playerTableRow, PCollection<TableRow> paymentCategoryTableRow) throws Exception {
	  
	  PCollection<KV<String, String>> playerInfo = playerTableRow.apply(ParDo.of(new ExtractPlayerDataFn()));
	  PCollection<KV<String, String>> paymentCategoryInfo = paymentCategoryTableRow.apply(ParDo.of(new ExtractPlayerTeamInfoFn()));
	  
	  PCollection<KV<String, KV<String, String>>> joinOutCollection = Join.rightOuterJoin(paymentCategoryInfo,playerInfo,"");
	  PCollection<String> finalFormattedResult = joinOutCollection.apply(ParDo.of(new extractResultString()));
	  
	  return finalFormattedResult;
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
	PCollection<TableRow> paymentCategoryTableRow = pipeline.apply(BigQueryIO.readTableRows().from(paymentCategory));
		
	//Joins
	PCollection<String> finalResultsIJ = innerJoin(playerTableRow, paymentCategoryTableRow);	
	PCollection<String> finalResultsOJ = outerJoin(playerTableRow, paymentCategoryTableRow);
	PCollection<String> finalResultsLJ = lftJoin(playerTableRow, paymentCategoryTableRow);
	PCollection<String> finalResultsRJ = rghtJoin(playerTableRow, paymentCategoryTableRow);
	
	
	//write to a file	
	finalResultsIJ.apply("Write To Text",
    	    TextIO.write().to(targetFileLocation+"InnerJoin_").withSuffix(".csv").withNumShards(1));	
	finalResultsOJ.apply("Write To Text",
    	    TextIO.write().to(targetFileLocation+"OuterJoin_").withSuffix(".csv").withNumShards(1));
	finalResultsLJ.apply("Write To Text",
    	    TextIO.write().to(targetFileLocation+"LeftJoin_").withSuffix(".csv").withNumShards(1));
	finalResultsRJ.apply("Write To Text",
    	    TextIO.write().to(targetFileLocation+"RightJoin_").withSuffix(".csv").withNumShards(1));
	
	
	pipeline.run().waitUntilFinish();
  }
}
