package etl.loader

data class DataLoaderParameters(val projectId: String,
                                val dataset: String,
                                val tableId: String,
                                val inputFilePath: String)
