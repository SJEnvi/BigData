from mrjob.job import MRJob

def split_ignore_commas_in_quotes(line):
    result = []
    i = 0
    in_quote = False
    for j, c in enumerate(line):
        if c == '"':
            in_quote = not in_quote
        if c == ',' and not in_quote:
            result.append(line[i:j])
            i = j + 1
    result.append(line[i:])
    return result

class MRHotelRaitingCount(MRJob):

    def mapper(self, _, line):
        row = split_ignore_commas_in_quotes(line)
        try:
            if len(row) == 4:
                (userId, movieId, rating, timestamp) = row
                yield movieId, ("Rating", float(rating))
            else:
                (movieId, title, genres) = row
                yield movieId, ("Title", title)
        except:
            pass


    def reducer(self, movieID, value):
        movieTitle = ""
        new_value = []
        counter = 0

        for i in value:
            if i[0] == "Title":
                movieTitle = i[1]
            else:
                new_value.append(i[1])

        try:
            yield movieTitle, round(sum(new_value)/len(new_value), 2)
        except:
            pass

if __name__ == '__main__':
    MRHotelRaitingCount.run()
