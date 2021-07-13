package dataflow;

import java.io.Serializable;

import org.apache.beam.sdk.transforms.Combine.CombineFn;

public class CombinerMeanFn extends CombineFn<Integer, CombinerMeanFn.Accumulator, Double>{
	
	public static class Accumulator implements Serializable{
		Double sum = 0.0;
		Integer count = 0;
	}

	@Override
	public Accumulator createAccumulator() {
		return new Accumulator();
	}
	
	@Override
	public Accumulator addInput(Accumulator acc, Integer in) {
		acc.sum += in;
		acc.count += 1;
		return acc;
	}
	
	@Override
	public Accumulator mergeAccumulators(Iterable<Accumulator> accs) {
		Accumulator mergeAcc = new Accumulator();
		accs.forEach(acc -> {
			mergeAcc.sum = mergeAcc.sum + acc.sum;
			mergeAcc.count = mergeAcc.count + acc.count;
		});
		return mergeAcc;
	}
	
	@Override
	public Double extractOutput(Accumulator mergedAcc) {
		if(mergedAcc.count > 0)
			return mergedAcc.sum/mergedAcc.count;
		else
			return Double.NaN;
	}
}
