package dataflow;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.CoGroup;
import org.apache.beam.sdk.schemas.transforms.CoGroup.By;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;

public class CoGroupExample {
	
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String> deptData = pipeline.apply("read dep_data", 
				TextIO.read().from("C:\\Users\\sourav.kumar\\Documents\\Dataflow\\input\\dept_data.txt"));
		PCollection<String> locData = pipeline.apply("read dep_data", 
				TextIO.read().from("C:\\Users\\sourav.kumar\\Documents\\Dataflow\\input\\location.txt"));
		
		PCollection<KV<String, List<String>>> deptDataMap = deptData
				.apply("split", MapElements
						.into(TypeDescriptors.lists(TypeDescriptors.strings()))
						.via((String elem) -> Arrays.asList(elem.split(","))))
				.apply("create map", MapElements
						.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.lists(TypeDescriptors.strings())))
						.via(elem -> KV.<String, List<String>>of(elem.get(0), elem)))
				.apply("groupByKey", GroupByKey.create())
				.apply("ExtractFirstElem", MapElements
						.into(TypeDescriptors.lists(TypeDescriptors.strings()))
						.via(elem -> elem.getValue().iterator().next()))
				.apply("create map", MapElements
						.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.lists(TypeDescriptors.strings())))
						.via(elem -> KV.<String, List<String>>of(elem.get(0), elem)));
		
		PCollection<KV<String, List<String>>> locDataMap = locData
				.apply("split", MapElements
						.into(TypeDescriptors.lists(TypeDescriptors.strings()))
						.via((String elem) -> Arrays.asList(elem.split(","))))
				.apply("create map", MapElements
						.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.lists(TypeDescriptors.strings())))
						.via(elem -> KV.<String, List<String>>of(elem.get(0), elem)));
		
		TupleTag<List<String>> deptTag = new TupleTag<>();
		TupleTag<List<String>> locTag = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> result = 
				KeyedPCollectionTuple.of(deptTag, deptDataMap)
				.and(locTag, locDataMap)
				.apply(CoGroupByKey.create());
		
		result
		.apply("parse cogroup", ParDo.of(new DoFn<KV<String, CoGbkResult>, String>(){
			@ProcessElement
			public void processElement(ProcessContext c) {
				KV<String, CoGbkResult> elem = c.element();
				String key = elem.getKey();
				Iterable<List<String>> deptList = elem.getValue().getAll(deptTag);
				Iterable<List<String>> locList = elem.getValue().getAll(locTag);
				
				String deptListformattedOutput = key + ":" + "dept-" + deptList.toString() + "loc-" + locList.toString();
				c.output(deptListformattedOutput);
			}
		}))
		.apply("write output", ParDo.of(new WriteToConsoleFn()));
		
		pipeline.run().waitUntilFinish();
		
	}

}
