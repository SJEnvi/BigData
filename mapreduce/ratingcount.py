from statistics import mean
from mrjob.job import MRJob

class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        try:
            (userId, movieId, rating, timestamp) = line.split(",")
            result = [movieId, float(rating)]
            yield result
        except:
            pass

    def reducer(self, key, value):
        result = [key, mean(value)]
        yield result

if __name__ == '__main__':
    MRHotelRaitingCount.run()