from abc import ABC, abstractmethod


class EtlJob(ABC):

    @abstractmethod
    def extract_data(self, spark):
        pass

    @abstractmethod
    def transform_data(self, spark, df):
        pass

    @abstractmethod
    def load_data(self, df):
        pass
