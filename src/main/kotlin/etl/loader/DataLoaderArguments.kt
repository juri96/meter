package etl.loader

import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

fun buildArgumentOptions() = Options().also {
    it.addArgument("runner", "runner", "Apache Beam runner")
    it.addArgument("project", "project", "GCP project name")
    it.addArgument("input", "input", "CSV input files")
    it.addArgument("job_name", "job_name", "Job name")
    it.addArgument("dataset", "dataset", "Big Query dataset name")
    it.addArgument("temp_location", "temp_location", "Dataflow temp location (path to any GCP bucket eg. gs://INPUT_BUCKET/temp)")
    it.addArgument("staging_location", "staging_location", "Dataflow staging location (path to any GCP bucket eg. gs://INPUT_BUCKET/staging)")
    it.addArgument("table_name", "table_name", "Big Query result table name")
}

private fun Options.addArgument(argName: String, longOpt: String, description: String) =
        addOption(Option.builder().argName(argName).longOpt(longOpt).hasArg().desc(description).required().build())
