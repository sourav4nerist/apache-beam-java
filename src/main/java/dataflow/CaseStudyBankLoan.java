package dataflow;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.collect.Lists;

public class CaseStudyBankLoan {
	
	static DateTimeFormatter formatter = DateTimeFormat.forPattern("dd-MM-yyyy");
	
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		PCollection<String> loanData = pipeline
				.apply("ReadCardData",
						TextIO.read().from("C:\\Users\\sourav.kumar\\Documents\\Dataflow\\input\\bank\\loan.txt"))
				.apply("filterHeader", ParDo.of(new DoFn<String, String>() {
					@ProcessElement
					public void processElement(ProcessContext ctx) {
						if (!ctx.element().startsWith("#"))
							ctx.output(ctx.element());
					}
				}));
		PCollection<List<String>> spiltLoanData = loanData
				.apply("createList", MapElements.<String, List<String>>via(new SimpleFunction<String, List<String>>(){
					@Override
					public List<String> apply(String in){
						return Arrays.asList(in.split(","));
					}
				}));
		
		PCollection<List<String>> medicalLoan = spiltLoanData
				.apply("FilterMedicalLoan", Filter.by(elem -> elem.get(5).equals("Medical Loan")));
		PCollection<List<String>> personalLoan = spiltLoanData
				.apply("FilterPersonalLoan", Filter.by(elem -> elem.get(5).equals("Personal Loan")));
		
		PCollection<KV<String, Integer>> medicalDefaulters = medicalLoan
		.apply("FilterFields", MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
				.via(in-> Arrays.asList(in.get(0).trim(), in.get(6).trim(), in.get(8).trim())))
		.apply("MedicalLoanDefaulterFn", ParDo.of(new MedicalLoanDefaulterFn()))
		.apply("SumByCustId", Sum.integersPerKey())
		.apply("FilterDefaulters", Filter.by(kv -> kv.getValue() > 3));
		//.apply("Format", ToString.kvs(":Medical:"))
		//.apply("WriteToConsole", ParDo.of(new WriteToConsoleFn()));
		
		PCollection<KV<String, Integer>> personalDefaulters = personalLoan
		.apply("FilterFields", MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
				.via(in-> Arrays.asList(in.get(0).trim(), in.get(6).trim(), in.get(8).trim())))
		.apply("FetchMonth", MapElements.<List<String>, KV<String, Integer>>
								via (new SimpleFunction<List<String>, KV<String, Integer>>(){
									@Override
									public KV<String, Integer> apply(List<String> in){
										String custId = in.get(0);
										DateTime paymentDate = formatter.parseDateTime(in.get(2));
										return KV.of(custId, paymentDate.getMonthOfYear());
									}
								}))
		.apply("GroupByCustId", GroupByKey.create())
		.apply("PersonalLoanDefaulterFn", ParDo.of(new PersonalLoanDefaulterFn()));
		//.apply("format", ToString.kvs(":Personal:"))
		//.apply("writeToConsole", ParDo.of(new WriteToConsoleFn()));
		
		PCollectionList<KV<String, Integer>> defaulterList = PCollectionList.of(medicalDefaulters).and(personalDefaulters);
		
		defaulterList
		.apply("flattening/union Personal and Medical defaulters", Flatten.pCollections())
		.apply("put against singleKey", MapElements.via(new SimpleFunction<KV<String, Integer>, KV<String, KV<String, Integer>>>(){
			@Override
			public KV<String, KV<String, Integer>> apply(KV<String, Integer> in){
				return KV.of("SingleKey", in);
			}
		}))
		.apply("GroupByKey", GroupByKey.create())
		.apply("SortingByNoOfDefaultMonths", ParDo.of(new SortByValue()))
		//.apply("SortingByDefaultMonths", MapElements.via(new SortByValue()))
		/*.apply("ExtractOutput", MapElements.via(new SimpleFunction<List<KV<String, Integer>>, KV<String, Integer>>() {
			@Override
			public KV<String, Integer> apply(List<KV<String, Integer>> in){
				return KV.of(in.get(0).getKey(), in.get(0).getValue());
			}
		}))*/
		.apply("Format", ToString.kvs(":"))
		.apply("WriteToConsole", ParDo.of(new WriteToConsoleFn()));
		
		pipeline.run().waitUntilFinish();
	}
	
	public static class MedicalLoanDefaulterFn extends DoFn<List<String>, KV<String, Integer>>{
		@ProcessElement
		public void process(ProcessContext ctx) {
			List<String> elem = ctx.element();
			String custId = elem.get(0);
			DateTime dueDate = formatter.parseDateTime(elem.get(1));
			DateTime paymentDate = formatter.parseDateTime(elem.get(2));
			if(dueDate.isBefore(paymentDate)) ctx.output(KV.of(custId, 1));
		}
	}
	
	public static class PersonalLoanDefaulterFn extends DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>>{
		
		@ProcessElement
		public void processElement(ProcessContext ctx) {
			KV<String, Iterable<Integer>> elem = ctx.element();
			String custId = elem.getKey();
			List<Integer> monthsSorted = StreamSupport
					.stream(elem.getValue().spliterator(), false)
					.sorted()
					.collect(Collectors.toList());
			
			int missedPayments = 12 - monthsSorted.size();
			
			if(missedPayments >= 4) 
				ctx.output(KV.of(custId, missedPayments));
			else if(monthsSorted.get(0) > 2) 
				ctx.output(KV.of(custId, monthsSorted.get(0) - 1));
			else if(monthsSorted.get(monthsSorted.size()-1) < 11) 
				ctx.output(KV.of(custId, 12 - monthsSorted.get(monthsSorted.size()-1)));
			else {
				for(int i = 0; i < monthsSorted.size()-2; i++) {
					int missBetweenMonths = monthsSorted.get(i+1) - monthsSorted.get(i);
					if(missBetweenMonths >=2)
						ctx.output(KV.of(custId, missBetweenMonths));
				}
			}
			
		}
	}
	
	// This SimpleFunction implements works as well
	/*public static class SortByValue extends SimpleFunction<KV<String, Iterable<KV<String, Integer>>>, List<KV<String, Integer>>>{
		@Override
		public List<KV<String, Integer>> apply(KV<String, Iterable<KV<String, Integer>>> in){
			return Lists.newArrayList(in.getValue())
					.stream()
					.sorted((kv1, kv2) -> kv2.getValue().compareTo(kv1.getValue()))
					//.collect(Collectors.toList());
					.collect(Collectors.toMap(kv -> kv.);
		}*/
	
	public static class SortByValue extends DoFn<KV<String, Iterable<KV<String, Integer>>>, KV<String, Integer>>{
		@ProcessElement
		public void processElement(ProcessContext ctx){
			Lists.newArrayList(ctx.element().getValue())
					.stream()
					.sorted((kv1, kv2) -> kv2.getValue().compareTo(kv1.getValue()))
					.collect(Collectors.toList())
					.stream().forEach(item -> {
						ctx.output(KV.of(item.getKey(), item.getValue()));
					});
		}
	}
	
}
