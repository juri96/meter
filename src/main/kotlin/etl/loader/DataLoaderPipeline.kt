package etl.loader

import com.google.api.services.bigquery.model.TableReference
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO


object DataLoaderPipeline {

    fun process(pipeline: Pipeline, parameters: DataLoaderParameters): Pipeline {
        val resultTableSpec = TableReference()
                .apply {
                    projectId = parameters.projectId
                    datasetId = parameters.dataset
                    tableId = parameters.tableId
                }

        pipeline
                .apply("Read files", TextIO.read().from(parameters.inputFilePath))
                .apply("Filter headers", PipelineSteps.filterHeaderRow())
                .apply("Split lines", PipelineSteps.splitCsvLine())
                .apply("Map to entity", PipelineSteps.mapElementsToEntity())
                .apply("Map to table row", PipelineSteps.mapEntityToTableRow())
                .apply("Write to BigQuery", PipelineSteps.writeToBigQuery(resultTableSpec))

        return pipeline
    }
}