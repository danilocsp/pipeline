import unittest, os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.jobs.to_trusted import ToTrusted


class ToTrustedTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ToTrustedTest, self).__init__(*args, **kwargs)

    def test_process(self):
        # execute transformation job
        path = os.path.dirname(__file__)
        fr_path = '{}/../locallake/raw/students.csv'.format(path)
        to_path = '{}/../locallake/trusted/students'.format(path)

        processor = ToTrusted(fr_path, to_path)
        result = processor.process()

        # validade process output
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.format('delta').load(to_path)

        self.assertEqual(5, df.count())

        # lazy ops vc eager ops -- collect
        lines = df.groupBy(['genre']).count().orderBy(F.asc('count')).collect()
        self.assertEqual(2,lines[0]['count'])
        self.assertEqual(3,lines[1]['count'])
