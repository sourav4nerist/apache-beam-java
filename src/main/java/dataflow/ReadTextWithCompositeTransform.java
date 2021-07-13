package dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

import dataflow.config.DataflowConfigOptions;

public class ReadTextWithCompositeTransform {
	
	public static void main(String[] args) {
		DataflowConfigOptions options = PipelineOptionsFactory.fromArgs(args)//.withValidation()
				.as(DataflowConfigOptions.class);
		
		Pipeline pipeline = Pipeline.create(options);
		
		PCollection<String> inputLines = pipeline.apply("Read Input", 
				TextIO.read().from(options.getInputFilePath()));
		PCollection<List<String>> splitLineViaMap = inputLines
		.apply("Split by ,", MapElements
		.into(TypeDescriptors.lists(TypeDescriptors.strings()))
		.via(line -> Arrays.asList(line.split(","))));
		
		PCollection<List<String>> accountEntries = splitLineViaMap.apply("filter", Filter.by((List<String> in) -> in.get(3).equals("Accounts")));
		PCollection<List<String>> hrEntries = splitLineViaMap.apply("filter hr", Filter.by((List<String> in) -> in.get(3).equals("HR")));
		
		accountEntries
				.apply("composite transform", new CompositeTransform())
				.apply("format output",
						MapElements.into(TypeDescriptors.strings())
								.via((KV<String, Long> empCount) -> "Accounts: " + empCount.getKey() + ":" + empCount.getValue()))
				.apply("write output", ParDo.of(new WriteToConsoleFn()));
		
		hrEntries
		.apply("composite transform", new CompositeTransform())
		.apply("format output",
				MapElements.into(TypeDescriptors.strings())
						.via((KV<String, Long> empCount) -> "HR: " + empCount.getKey() + ":" + empCount.getValue()))
		.apply("write output", ParDo.of(new WriteToConsoleFn()));
		
		pipeline.run().waitUntilFinish();
		
	}

}
