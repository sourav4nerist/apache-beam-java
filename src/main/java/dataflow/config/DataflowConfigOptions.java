package dataflow.config;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface DataflowConfigOptions extends DataflowPipelineOptions{
	
	@Description("Input file")
	@Validation.Required
	ValueProvider<String> getInputFilePath();
	void setInputFilePath(ValueProvider<String> inputPath);
	
	@Description("Output file")
	ValueProvider<String> getOutputFilePath();
	void setOutputFilePath(ValueProvider<String> outputPath);
	
	@Description("Default value")
	@Default.Integer(1)
	ValueProvider<Integer> getDefaultValue();
	void setDefaultValue(ValueProvider<Integer> outputPath);
}
