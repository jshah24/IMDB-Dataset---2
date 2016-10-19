# IMDB-Dataset---2

Hadoop map-reduce to derive some statistics from IMDB movie data. 
There are 3 datafiles :: movies.dat, ratings.dat, users.dat 

Q1: Find top 5 average movies rated by female users and print out the titles and
the average rating given to the movie by female users.
This question involves filtering , joining data from multiple files and job chaining.

e.g
Given ratings.dat as (Note this is just an example data format for simplicity.)
userid movied ratings
1 30 4
1 40 3
2 20 3
2 30 4
3 30 2
users.dat as
userid gender
1 M
2 F
3 F
we can see from this data that user 2 and 3 are Females.
We can see movie id 30 is rated by users 2 and 3, therefore the average rating given to
the movieid with id 30 by female users is
4+2/2 = 3.
since user 2 and 3 are females and they rated the movie with id 30 ratings 4 and 2
respectively.

Q2.
Given the id of a movie, find all userids,gender and age of users who rated the
movie 4 or greater.
Use the users.dat and ratings.dat
