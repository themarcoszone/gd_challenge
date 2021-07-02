from pyspark import SparkContext, SparkConf, SQLContext
import logging

class Utils:
    def __init__(self):
        self.sc = SparkContext.getOrCreate(SparkConf())
        self.sqlContext = SQLContext(self.sc)
        self.logger = logging.getLogger(__name__)

    def read_parquet(self, base_path, *partitions):
        df = None
        if partitions:
            df = self.sqlContext.read \
                .option("basePath", base_path) \
                .parquet(*partitions)
        else:
            df = self.sqlContext.read.parquet(base_path)

        return df
