package dev.caraml.spark.results

import dev.caraml.spark.IngestionJobConfig

abstract class BaseResultStore {
    def storeResults(config: IngestionJobConfig, numRows: Long): Unit
}
