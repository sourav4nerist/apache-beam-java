/*package dataflow;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.model.pipeline.v1.RunnerApi.Trigger.AfterAll;
import org.apache.beam.model.pipeline.v1.RunnerApi.Trigger.AfterAny;
import org.apache.beam.model.pipeline.v1.RunnerApi.Trigger.AfterEach;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

import dataflow.WindowExample.CalculateProfit;

public class WindowComposite {
	private static final String SUBSCRIPTION = "projects/daas-edwi-keng-poc/subscriptions/edwi-topic-dev-2-sub";
	private static final String STORE_SALES_HEADER = "Store_id, Store_location, Product_id, Product_category, number_of_pieces_sold, buy_rate, sell_price,unix_timestamp";
	
	public static void main(String[] args) {
		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
		options.setStreaming(true);
		options.setProject("daas-edwi-keng-poc");
		Pipeline pipeline = Pipeline.create(options);
		pipeline
		.apply("Read from PubSub", PubsubIO.readStrings().fromSubscription(SUBSCRIPTION))
		.apply("FilterHeader", ParDo.of(new DoFn<String, String>() {
			@ProcessElement
			public void process(ProcessContext ctx) {
				String line = ctx.element();
				if(!line.equals(STORE_SALES_HEADER)) ctx.output(line);
			}
		}))
		.apply("SplitRecord", MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
				.via((String line) -> Arrays.asList(line.split(","))))
		.apply("FilterLocation", Filter.by(elem -> (elem.get(1).equals("Mumbai") || (elem.get(1).equals("Bangalore")))))
		.apply("CalculateProfit", ParDo.of(new CalculateProfit()))
		// We can skip WithTimestamp below, it will then take publisher timestamp of each message that pubsub puts by default
		.apply("PutTimestamp", WithTimestamps.of(elem -> Instant.ofEpochMilli(Long.parseLong(elem.get(1)))))
		.apply("KeyValueOfLocProfit", MapElements.via((new SimpleFunction<List<String>, KV<String, Long>>(){
			@Override
			public KV<String, Long> apply(List<String> in){
				return KV.of(in.get(1), Long.parseLong(in.get(8)));
			}
		})))
		// Fixed window example below with window of 20 sec
		.apply("FixedWindow20Sec", Window.<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(20)))
				// After first example below
			//	.triggering(
			//			AfterFirst.of(AfterPane.elementCountAtLeast(10),AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10)))))
				
				// After all example below
				.triggering(
						AfterAll.newBuilder().addSubtriggers(AfterPane.elementCountAtLeast(10))
						.addSubtriggers(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10)))))
				
				// After each example below
				//.triggering(
				//		AfterEach.inof(AfterPane.elementCountAtLeast(10),AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10)))))
				
				// After any example below
				//.triggering(
				//		AfterAny.of(AfterPane.elementCountAtLeast(10),AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10))))
		// Sliding window example below with window of 30 sec updated every 10 sec
		.apply("GroupByLoc", Sum.longsPerKey())
		.apply("format", ToString.kvs(":"))
		.apply("WriteToConsole", ParDo.of(new WriteToConsoleFn()));
		
		pipeline.run();
	}
	
	
	public static class CalculateProfit extends DoFn<List<String>, List<String>>{
		@ProcessElement
		public void calculate(ProcessContext ctx) {
			List<String> elem = ctx.element();
			int noOfItems = Integer.parseInt(elem.get(4));
			int buyRate = Integer.parseInt(elem.get(5));
			int sellPrice = Integer.parseInt(elem.get(6));
			int profit = noOfItems * (sellPrice - buyRate);
			elem.add(String.valueOf(profit));
			ctx.output(elem);
		}
	}
}
*/