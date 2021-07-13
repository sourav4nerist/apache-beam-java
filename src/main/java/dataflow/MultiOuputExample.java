package dataflow;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class MultiOuputExample {
	
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> deptData = pipeline.apply("read dep_data", 
				TextIO.read().from("C:\\Users\\sourav.kumar\\Documents\\Dataflow\\input\\dept_data.txt"));
		PCollection<String> excludeData = pipeline.apply("read dep_data", 
				TextIO.read().from("C:\\Users\\sourav.kumar\\Documents\\Dataflow\\input\\exclude.txt"));
		PCollectionView<List<String>> excludeView = excludeData.apply(View.asList());
		
		TupleTag<KV<String,Integer>> accountsTag = new TupleTag<KV<String,Integer>>(){};
		TupleTag<KV<String,Integer>> accountsExclusionTag = new TupleTag<KV<String,Integer>>(){};
		TupleTag<KV<String,Integer>> hrTag = new TupleTag<KV<String,Integer>>(){};
		
		PCollectionTuple tuple = deptData.apply("filter", ParDo.of(new DoFn<String, KV<String,Integer>>(){
			@ProcessElement
			public void processElement(ProcessContext ctx, MultiOutputReceiver out) {
				String deptData = ctx.element();
				List<String> excludeList = ctx.sideInput(excludeView);
				List<String> deptDataList = Arrays.asList(deptData.split(","));
				if(deptDataList.get(3).equals("Accounts")) {
					out.get(accountsTag).output(KV.of(deptDataList.get(0),1));
					if(!excludeList.contains(deptDataList.get(0))) {
						out.get(accountsExclusionTag).output(KV.of(deptDataList.get(0),1));
					}
				}
				else if (deptDataList.get(3).equals("HR")) {
					out.get(hrTag).output(KV.of(deptDataList.get(0),1));
				}
				
			}
		}).withSideInputs(excludeView)
				.withOutputTags(accountsTag, 
						TupleTagList.of(Arrays.asList(accountsExclusionTag, hrTag))));
		
		PCollection<KV<String,Integer>> accKV = tuple.get(accountsTag);
		PCollection<KV<String,Integer>> accExclusionKV = tuple.get(accountsExclusionTag);
		PCollection<KV<String,Integer>> hrKV = tuple.get(hrTag);
		
		accKV
		.apply("count", Count.perKey())
		.apply("format", ToString.kvs(":Acc:"))
		.apply("writeConsole", ParDo.of(new WriteToConsoleFn()));
		
		accExclusionKV
		.apply("count", Count.<String, Integer>perKey())
		.apply("format", ToString.kvs(":AccExc:"))
		.apply("writeConsole", ParDo.of(new WriteToConsoleFn()));
		
		hrKV
		.apply("count", Count.perKey())
		.apply("format", ToString.kvs(":HR:"))
		.apply("writeConsole", ParDo.of(new WriteToConsoleFn()));
		
		pipeline.run().waitUntilFinish();
	}

}
