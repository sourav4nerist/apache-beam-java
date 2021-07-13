package dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;

public class CombinerExample {
	
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		pipeline.apply("Input", Create.of(15,5,7,7,9,23,13,5))
		.apply("combine", Combine.globally(new CombinerMeanFn()).withFanout(2))
		.apply("write", ParDo.of(new DoFn<Double, Void>(){
			@ProcessElement
			public void process(ProcessContext c) {
				System.out.println(c.element());
			}
		}));
		
		pipeline.run().waitUntilFinish();
	}

}
