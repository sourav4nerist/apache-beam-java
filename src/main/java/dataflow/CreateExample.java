package dataflow;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class CreateExample {
	
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		pipeline.apply("Create", Create.of(Arrays.asList(1,2,3,4)))
		.apply("print", ParDo.of(new DoFn<Integer, Void>(){
			@ProcessElement
			public void processElement(ProcessContext c) {
				System.out.println(c.element());
			}
		}));
		
		pipeline.run().waitUntilFinish();
	}

}
