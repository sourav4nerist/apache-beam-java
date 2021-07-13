package dataflow;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;

import com.google.common.util.concurrent.ClosingFuture.Combiner;

public class SideInputExample {
	
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String> deptData = pipeline.apply("read dep_data", 
				TextIO.read().from("C:\\Users\\sourav.kumar\\Documents\\Dataflow\\input\\dept_data.txt"));
		PCollection<String> excludeData = pipeline.apply("read dep_data", 
				TextIO.read().from("C:\\Users\\sourav.kumar\\Documents\\Dataflow\\input\\exclude.txt"));
		PCollectionView<List<String>> excludeView = excludeData.apply(View.asList());
		
		deptData.apply("filter", ParDo.of(new DoFn<String, KV<String,Integer>>(){
			@ProcessElement
			public void processElement(ProcessContext ctx) {
				String deptData = ctx.element();
				List<String> excludeList = ctx.sideInput(excludeView);
				List<String> deptDataList = Arrays.asList(deptData.split(","));
				if(deptDataList.get(3).equals("Accounts") && !excludeList.contains(deptDataList.get(0))) {
					ctx.output(KV.of(deptDataList.get(0),1));
				}
				
			}
		}).withSideInputs(excludeView))
		.apply("count", Count.perKey())
		.apply("format", ToString.kvs(":"))
		.apply("writeConsole", ParDo.of(new WriteToConsoleFn()));
		
		pipeline.run().waitUntilFinish();
		
	}

}
