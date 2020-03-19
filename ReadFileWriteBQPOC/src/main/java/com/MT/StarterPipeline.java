package com.MT;

import com.google.api.services.bigquery.model.TableRow;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

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
  private static final String sourceFile = "gs://my_bucket_8627/VZT/Inbound/VGT_Source.csv";
  private static final String Project = "mt-poc";
  private static final String StagingLocation = "gs://my_bucket_8627/VZT/Inbound/";
  private static final String TempLocation = "gs://my_bucket_8627/tmp/BigQueryExtractTemp";
  private static final String IngestTable = "mt-poc:Examples.VZT_DS";
  
  
  static class stringToTableRow extends DoFn<String, TableRow> {
	  /**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@ProcessElement
	  public void processElement(ProcessContext c) {
	      DateFormat currentDate = new SimpleDateFormat("yyyy-MM-dd");
	     
	      //Tokenize the row based on comma
	      String[] attribute = c.element().split(",");

	      //Create Table Row
	      TableRow row = new TableRow();

			  row.set("IngestionDate","03-19-2020");
			  row.set("VGT_Datashare_ID",attribute[0]);
			  row.set("LOB",attribute[1]);
			  row.set("BTN",attribute[2]);
	          
			  System.out.println("Rows Of Record "+row.toString());
			  
	      c.output(row);

	    }// Function Ends
	  }//Class Ends
 
  public static void main(String[] args) {
  	  
	  // Create and set your PipelineOptions.
	  DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
	  	
	  options.setProject(Project);
	  options.setStagingLocation(StagingLocation);
	  options.setTempLocation(TempLocation);
	  
	// Create the Pipeline with the specified options.
	Pipeline pipeline = Pipeline.create(options);
	
	//Read Rows Of File
	PCollection<String> eachRecord = pipeline.apply("Read from CSV", TextIO.read().from(sourceFile));
	PCollection<TableRow> VZT_DS = eachRecord.apply("Extract Each Row", ParDo.of(new stringToTableRow()));
	
	//Ingest Into Big Query Table
    VZT_DS.apply(BigQueryIO
    		.writeTableRows()
    		.to(IngestTable)
    		.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    		.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
    		.withoutValidation());

    pipeline.run().waitUntilFinish();
  }
  
  


}
