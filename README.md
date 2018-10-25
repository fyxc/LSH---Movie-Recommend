# LSH---Movie-Recommend

In the sample. there are 100 different movies, numbered from 0 to 99.A user is represented as a set of movies.for each user U, find top-5 users who are most similar to U (by their Jaccard similarity, if same, choose the user that has smallest ID), and recommend top-3 movies to U where the movies are ordered by the number of these top-5 users who have watched them (if same, choose the movie that has the smallest ID).


Assumption:
1. For each user, obtain a signature of 20 values
2. i-th hash function for the signature: h(x,i) = (3x + 13i) % 100
3. the signature is divided into 5 bands, with 4 values in each band
