package dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class CodersExample {
	
	public static void main(String[] args) throws @UnknownKeyFor @NonNull @Initialized CannotProvideCoderException {
		
		System.out.println("List of Deafult coders");
		System.out.println("======================================================");
		Coder stringCoder = CoderRegistry.createDefault().getCoder(TypeDescriptors.strings());
		System.out.println("String:   " + stringCoder.toString());
		
		Coder integerCoder = CoderRegistry.createDefault().getCoder(TypeDescriptors.integers());
		System.out.println("Integer:   " + integerCoder.toString());
		
		Coder doubleCoder = CoderRegistry.createDefault().getCoder(TypeDescriptors.doubles());
		System.out.println("Double:   " + doubleCoder.toString());
		
		Coder longCoder = CoderRegistry.createDefault().getCoder(TypeDescriptors.longs());
		System.out.println("Long:   " + longCoder.toString());
		
		Coder booleanCoder = CoderRegistry.createDefault().getCoder(TypeDescriptors.booleans());
		System.out.println("Boolean:   " + booleanCoder.toString());
		
		Coder floatCoder = CoderRegistry.createDefault().getCoder(TypeDescriptors.floats());
		System.out.println("Float:   " + floatCoder.toString());
		
		Coder kvCoder = CoderRegistry.createDefault().getCoder(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()));
		System.out.println("KV<String, Integer>:   " + kvCoder.toString());
		
		Coder listCoder = CoderRegistry.createDefault().getCoder(TypeDescriptors.lists(TypeDescriptors.strings()));
		System.out.println("List<String>:   " + listCoder.toString());
		
		Coder mapCoder = CoderRegistry.createDefault().getCoder(TypeDescriptors.lists(TypeDescriptors.maps(TypeDescriptors.strings(), TypeDescriptors.integers())));
		System.out.println("Map<String, Integer>:   " + mapCoder.toString());
		
		System.out.println("======================================================");
		System.out.println("Modify/add/update coder");
		System.out.println("======================================================");
		
		Pipeline p = Pipeline.create();
		p.setCoderRegistry(CoderRegistry.createDefault());
		p.apply("coder", Create.of(1,2))
		.setCoder(BigEndianIntegerCoder.of());

	}

}
