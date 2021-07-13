package dataflow;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Regex;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.TypeDescriptors;

import dataflow.config.DataflowConfigOptions;

public class WordCount {
	
	public static void main(String[] args) {
		DataflowConfigOptions options = PipelineOptionsFactory.fromArgs(args).as(DataflowConfigOptions.class);
		Pipeline pipeline = Pipeline.create(options);
		pipeline
		.apply("readFile", 
				TextIO.read()
				.from(options.getInputFilePath()))
		/*.apply("extractWords", 
				FlatMapElements
				.into(TypeDescriptors.strings())
				.via((String in) -> Arrays.asList(in.split(","))));*/
		.apply("extractWords", Regex.split("\\W+"))
		.apply("countWords", Count.perElement())
		.apply("formatOutput", ToString.kvs())
		//.apply("writeOutput", TextIO.write().to("/Users/sourav.kumar/Documents/Dataflow/output/wordCount"));
		.apply("printOutput", ParDo.of(new DoFn<String, Void>(){
			@ProcessElement
			public void processElement(ProcessContext c) {
				System.out.println(c.element());
			}
		}));
		
		pipeline.run().waitUntilFinish();
	}

}
