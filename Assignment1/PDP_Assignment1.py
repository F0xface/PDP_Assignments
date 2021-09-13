from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieRatingSorter(MRJob):

	# Define the steps that will be executed in order
	def steps(self):
		return [
			MRStep(mapper=self.mapper_get_movies, combiner=self.combine_movies, reducer=self.reducer_sum_ratings),
			MRStep(reducer=self.reducer_sort_by_rating)
		]

	# Get movieID from all the movies
	def mapper_get_movies(self, _, line):
		(userID, movieID, rating, timestamp) = line.split('\t')
		yield movieID, 1
	
	# Combine the number of ratings with the movieID
	def combine_movies(self, rating, counts):
		yield rating, sum(counts)
	
	# Sum all the ratings per movie
	def reducer_sum_ratings(self, key, values):
		yield None, (sum(values), key)
	
	# Sort the movies by rating
	def reducer_sort_by_rating(self, _, rating_counts):
		for count, key in sorted(rating_counts, reverse=True):
			yield (key, int(count))

# Execute the file
if __name__ == '__main__':
	MovieRatingSorter.run()