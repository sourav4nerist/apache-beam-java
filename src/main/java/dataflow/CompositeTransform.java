package dataflow;

import java.util.List;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class CompositeTransform extends PTransform<PCollection<List<String>>, PCollection<KV<String, Long>>>{
	
	@Override
	public PCollection<KV<String, Long>> expand(PCollection<List<String>> in) {
		return 
		in
		.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
				.via((List<String> entries) -> KV.of(entries.get(1), 1)))
		.apply("countByEmp", Count.perKey())
		.apply("filter > 30", Filter.by((KV<String, Long> elem) -> elem.getValue() > 30));
	}

}
