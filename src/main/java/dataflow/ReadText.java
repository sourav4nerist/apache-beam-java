package dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dataflow.config.DataflowConfigOptions;

public class ReadText {
	private static final Logger LOGGER = LoggerFactory.getLogger(ReadText.class);

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
		
		PCollection<String> acc = accountEntries
				.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
						.via((List<String> entries) -> KV.of(entries.get(1), 1)))
				.apply("countByEmp", Count.perKey())
				.apply("format output",
						MapElements.into(TypeDescriptors.strings())
								.via((KV<String, Long> empCount) -> "Accounts: " + empCount.getKey() + ":" + empCount.getValue()));
				//.apply("write account output", TextIO.write().to("/Users/sourav.kumar/Documents/Dataflow/output/EmpAttendanceAccount"));
		
		PCollection<String> hr = hrEntries
		.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
				.via((List<String> entries) -> KV.of(entries.get(1), 1)))
		.apply("countByEmp", Count.perKey())
		.apply("format output",
				MapElements.into(TypeDescriptors.strings())
						.via((KV<String, Long> empCount) -> "HR: " + empCount.getKey() + ":" + empCount.getValue()));
		//.apply("write hr output", TextIO.write().to("/Users/sourav.kumar/Documents/Dataflow/output/EmpAttendanceHR"));
		
		PCollectionList.of(acc).and(hr).apply("union acc and hr", Flatten.pCollections())
		.apply("write output", TextIO.write().to("/Users/sourav.kumar/Documents/Dataflow/output/AccountsHrUnion"));
		
		accountEntries
		.apply("count", Count.globally())
		.apply("out to String type", MapElements.into(TypeDescriptors.strings()).via(count -> count.toString()))
		.apply("print output", TextIO.write().to("/Users/sourav.kumar/Documents/Dataflow/output/AccountsEntryTotal"));
		
		
		// Extracting account dept records using ParDO
		
		inputLines
		.apply("split", ParDo.of(new DoFn<String, List<String>>(){
			@ProcessElement
			public void process(ProcessContext c){
				c.output(Arrays.asList(c.element().split(",")));
			}
		}))
		.apply("filter", ParDo.of(new DoFn<List<String>, List<String>>(){
			@ProcessElement
			public void processElement(ProcessContext c){
				if(c.element().get(3).equals("Accounts")) {
					c.output(c.element());
				}
			}
		}))
		.apply("MarkEmployeeEachDay", ParDo.of(new DoFn<List<String>, KV<String, Integer>>() {
			@ProcessElement
			public void processElement(ProcessContext c){
				c.output(KV.of(c.element().get(1),1));
			}
		}))
		//.apply("GroupByKey", GroupByKey.create())
		.apply("SumPerKey", Sum.integersPerKey())
		/*.apply("SumTotalPerEmp", ParDo.of(new DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>>() {
			@ProcessElement
			public void process(ProcessContext c){
				int add = 0;
				for(Integer num: c.element().getValue()) {
					add += num;
				}
				c.output(KV.of(c.element().getKey(), add));
			}
		}))*/
		.apply("formatOutput", ToString.kvs())
		.apply("print", ParDo.of(new DoFn<String, Void>(){
			@ProcessElement
			public void processElement(ProcessContext c) {
				System.out.println(c.element());
			}
		}));
		
		pipeline.run().waitUntilFinish();
	}

}
