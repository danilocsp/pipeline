from pyspark.sql import SparkSession
from src.commons.abstract_job import AbstractJob

class ToTrusted(AbstractJob):

    def __init__(self, fr_path: str, to_path: str):
        super(ToTrusted, self).__init__(fr_path, to_path)


    def process(self):
        spark = SparkSession.builder.getOrCreate()

        # read from raw zone
        df = spark.read.format('csv').option('header',True).load(self.fr_path)

        # drop nullable records
        df = df.na.drop()

        # write data to trusted
        df.write.format('delta').mode('overwrite').save(self.to_path)

if __name__ =='__main__':
    ft_path, to_path = sys.argv[1:]
    processor = ToTrusted(fr_path, to_path)
    processor.process()
