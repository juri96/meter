package etl.loader

import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.commons.cli.DefaultParser

fun main(args: Array<String>) {
    val cmd = DefaultParser().parse(buildArgumentOptions(), args)

    val options = PipelineOptionsFactory.`as`(DataflowPipelineOptions::class.java).apply {
        project = cmd.getOptionValue("project")
        stagingLocation = cmd.getOptionValue("staging_location")
        tempLocation = cmd.getOptionValue("temp_location")
        jobName = cmd.getOptionValue("job_name")
        runner = DataflowRunner::class.java
    }

    val pipeline = Pipeline.create(options)
    DataLoaderPipeline.process(pipeline, DataLoaderParameters(
            dataset = cmd.getOptionValue("dataset"),
            inputFilePath = cmd.getOptionValue("input"),
            projectId = cmd.getOptionValue("project"),
            tableId = cmd.getOptionValue("table_name")
    )).run()
}
