package dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import com.google.common.collect.Lists;

public class CaseStudyCreditCard {
	
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> cardData = pipeline
				.apply("ReadCardData", TextIO.read()
						.from("C:\\Users\\sourav.kumar\\Documents\\Dataflow\\input\\bank\\cards.txt"));
		cardData
		.apply("filterHeader", ParDo.of(new DoFn<String, String>(){
			@ProcessElement
			public void processElement(ProcessContext ctx) {
				if(!ctx.element().startsWith("#"))
					ctx.output(ctx.element());
			}
		}))
		.apply("split", MapElements
				.into(TypeDescriptors.lists(TypeDescriptors.strings()))
				.via(elem -> Arrays.asList(elem.split(","))))
		.apply("ScorePerRecord", ParDo.of(new DoFn<List<String>, KV<String, Integer>>(){
			@ProcessElement
			public void processElement(ProcessContext ctx) {
				List<String> rec = ctx.element();
				int defraudScore = 0;
				int maxLimit = Integer.parseInt(rec.get(5));
				int amtSpent = Integer.parseInt(rec.get(6));
				int amtPaid = Integer.parseInt(rec.get(8));
				// if amount paid is less than 70% of amount spent, add 1 point.
				if(amtPaid < amtSpent*.7) defraudScore ++;
				// if amount spent = max limit and amount paid = amount spent, add 1 point.
				if(amtSpent == maxLimit && amtPaid < amtSpent) defraudScore ++;
				// both rule failed, add 1 point.
				if(amtSpent == maxLimit && amtPaid < (amtSpent*.7)) defraudScore ++;
				
				ctx.output(KV.of(rec.get(0), defraudScore));
			}
		}))
		.apply("SumOfPoint",Sum.integersPerKey())
		.apply("FilterNonDefraudCases", Filter.by((KV<String, Integer> kv) -> kv.getValue() !=0))
		// Logic to sort by defraud points in desc  -- start
		.apply("AddSingleKey", MapElements
				.via(new SimpleFunction<KV<String, Integer>, KV<String, KV<String, Integer>>>(){
					@Override
					public KV<String, KV<String, Integer>> apply(KV<String, Integer> in) {
						return KV.of("single", in);
					}
			
		}))
		.apply("GroupByKeyToGetIterable", GroupByKey.create())
		.apply("Sorting", MapElements
				.via(new SimpleFunction<KV<String, Iterable<KV<String, Integer>>>, String>(){
					@Override
					public String apply(KV<String, Iterable<KV<String, Integer>>> in){
						Iterable<KV<String, Integer>> input = in.getValue();
						//Stream<KV<String, Integer>> kvStream = Lists.newArrayList(input.iterator())
						return Lists.newArrayList(input.iterator())
							   .stream()
							   .sorted((kv1, kv2) -> kv2.getValue().compareTo(kv1.getValue()))
							   .collect(StringBuilder::new,
									   (sb,kv) -> sb.append(String.format("%s : %d%n", kv.getKey(), kv.getValue())),
									   (sb,kv) -> {}
									   ).toString();
						
					}
				}))
		// Logic to sort by defraud points in desc  -- end
		//.apply("Format", ToString.kvs(":"))
		.apply("WriteToConsole", ParDo.of(new WriteToConsoleFn()));
		
		pipeline.run().waitUntilFinish();
	}

}
