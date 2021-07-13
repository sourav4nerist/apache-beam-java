package dataflow;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.CoGroup;
import org.apache.beam.sdk.schemas.transforms.CoGroup.By;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class BeamSQLExample {
	
	private static DateTimeFormatter formatter = DateTimeFormat.forPattern("dd-MM-yyyy");
	
	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String> deptData = pipeline.apply("read dep_data", 
				TextIO.read().from("C:\\Users\\sourav.kumar\\Documents\\Dataflow\\input\\dept_data.txt"));
		PCollection<String> locData = pipeline.apply("read dep_data", 
				TextIO.read().from("C:\\Users\\sourav.kumar\\Documents\\Dataflow\\input\\location.txt"));
		
		PCollection<List<String>> deptDataDistinctList = deptData
				.apply("split", MapElements
						.into(TypeDescriptors.lists(TypeDescriptors.strings()))
						.via((String elem) -> Arrays.asList(elem.split(","))))
				.apply("create map", MapElements
						.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.lists(TypeDescriptors.strings())))
						.via(elem -> KV.<String, List<String>>of(elem.get(0), elem)))
				.apply("groupByKey", GroupByKey.create())
				.apply("ExtractFirstElem", MapElements
						.into(TypeDescriptors.lists(TypeDescriptors.strings()))
						.via(elem -> elem.getValue().iterator().next()));
				
				
				
		PCollection<List<String>> locDataList = locData
				.apply("split", MapElements
						.into(TypeDescriptors.lists(TypeDescriptors.strings()))
						.via((String elem) -> Arrays.asList(elem.split(","))));
		
		Schema deptSchema = Schema.builder()
				.addStringField("emp_id")
				.addStringField("name")
				.addStringField("dept_id")
				.addStringField("dept_name")
				//.addDateTimeField("punch_date")
				.build();
		
		Schema locSchema = Schema.builder()
				.addStringField("emp_id")
				.addStringField("mobile")
				.addStringField("city")
				.build();
		
		PCollection<Row> deptRow = deptDataDistinctList
				.apply("createRow", ParDo.of(new DoFn<List<String>, Row>(){
					@ProcessElement
					public void process(ProcessContext ctx) {
						List<String> elem = ctx.element();
						Row row = Row.withSchema(deptSchema)
								//.addValues(elem)
								.addValue(elem.get(0))
								.addValue(elem.get(1))
								.addValue(elem.get(2))
								.addValue(elem.get(3))
							//	.addValue(formatter.parseDateTime(elem.get(4)))
								.build();
						ctx.output(row);
					}
				})).setRowSchema(deptSchema);
		
		PCollection<Row> locRow = locDataList
				.apply("createRow", ParDo.of(new DoFn<List<String>, Row>(){
					@ProcessElement
					public void process(ProcessContext ctx) {
						List<String> elem = ctx.element();
						Row row = Row.withSchema(locSchema)
								//.addValues(elem)
								.addValue(elem.get(0))
								.addValue(elem.get(1))
								.addValue(elem.get(2))
								.build();
						ctx.output(row);
					}
				})).setRowSchema(locSchema);
		
		TupleTag<Row> deptTag = new TupleTag<>();
		TupleTag<Row> locTag = new TupleTag<>();
		
		PCollectionTuple pct = PCollectionTuple.of(deptTag, deptRow)
				.and(locTag, locRow);
		
		pct.apply("join", CoGroup.join(By.fieldNames("emp_id")))
		.apply("format", ParDo.of(new DoFn<Row, String>(){
			@ProcessElement
			public void processElement(ProcessContext ctx) {
				Row row = ctx.element();
				StringBuffer buff = new StringBuffer();
				for(int i = 0; i<row.getFieldCount();i++) {
					if(i == 0) {
						Row inRow = row.getRow(i);
						buff.append(inRow.getString("emp_id"));
					}
					else if(i == 1) {
						Collection<Row> obj = row.getArray(i);
						obj.forEach(empRow -> {
							buff.append(",");
							buff.append(empRow.getString("name"));
							buff.append(",");
							buff.append(empRow.getString("dept_name"));
						});
					}
					else {
						Collection<Row> obj = row.getArray(i);
						obj.forEach(locRow -> {
							buff.append(",");
							buff.append(locRow.getString("mobile"));
							buff.append(",");
							buff.append(locRow.getString("city"));
						});
					}
				}
				ctx.output(buff.toString());
			}
		}))
		//.apply("format", ToString.elements())
		.apply("writeConsole", ParDo.of(new WriteToConsoleFn()));
		
		pipeline.run().waitUntilFinish();
		
	}

}
