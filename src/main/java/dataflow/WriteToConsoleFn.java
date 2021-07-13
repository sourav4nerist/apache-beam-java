package dataflow;

import org.apache.beam.sdk.transforms.DoFn;

@SuppressWarnings("serial")
public class WriteToConsoleFn extends DoFn<String, Void>{
	
	@ProcessElement
	public void process(ProcessContext c) {
		System.out.println(c.element());
	}
}
