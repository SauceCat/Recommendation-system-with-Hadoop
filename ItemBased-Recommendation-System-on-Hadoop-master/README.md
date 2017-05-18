# ItemBased Recommendation System on Hadoop

This is an itembased recommendation system framework built on Hadoop. It was instructed by the famous book <Mahout in Action>. 
There are some modifications according to the requirements of the project.  
Anyway, I made the whole framework clearer and easiler to use. :see_no_evil:   
  
##Data
This project was based on the famous Million Song Dataset. The Million Song Dataset is a freely available collection of metadata for millions contemporary songs. 
The complete datasets consist of 7 subsets of derived features. The total size of the dataset is 280GB.
The subset of data used in the challenge is called Taste Profile Subset that consists of more than 48 million triplets gathered from users' listening history. 
The triplets have 3 fields: user, song, counts. 
In total, the data consists of about 1.2 million users and covers more than 380,000 songs in MSD. 
Taste Profile Subset is used for collaborative filtering in this project.  
You can get the dataset here: [Million Song Dataset](http://labrosa.ee.columbia.edu/millionsong/)  

##Data preparetion
The whole taste profile dataset contains 1,019,318 unique users, 384,546 unique MSD songs 
and totally 48,373,586 in user-song-play count triplets format.
What I do is to make a set of unique users from the whole triplets, 
hash the unique users set using numeric value and finally match back to the original dataset. 
The process for the unique song set is the same.

##Basic stage description
**Stage One:** Generalise User-Item Preference vector  
**Stage Two:** Generate Item-User Preference vector We keep this Item-User Preference vector as the denominator part of the cosine similarity between items.  
**Stage Three:** Count item-item cooccurrence number From each User-Item Preference vector, 
we can generate item-item cooccurrence pairs. In the reduce stage, the total number of cooccurrence for each item pairs are calculated.  
Map Input: (songOne, songTwo)  
Reduce Output: (songOne, songTwo@cooccurrence count)   
**Stage Four and Five:** Normalise cooccurrence count for each pair We can 
consider the cooccurrence count of each item pair as the numerator of the 
cosine similarity of these two items. In order to get the accurate cosine similarity, 
we have to divide the numerator by denominator, which has been generated in stage two.  
**Stage Six:** Generate item-item similarity vector This stage summarise outcomes from stage five into item-item similarity vectors.  
Output: (SongOne, {SongTwo: similarity, SongThree: sim- ilarity, ...})  
**Stage Seven:** Generate target User-Item Preference vector  
**Stage Eight:** Generate (ItemID, similarity vector, userID, user-item prefer value) tuples Based 
on similarity vectors and the target user preference vectors, 
we can group these two vectors by itemID and then generate tuples 
for further use. We utilise the VectorOrPrefWritable class and Vec- torAndPrefWritable 
class from the Mahout library to store these kind of tuples.  
Output: (SongOne, ({SongTwo: similarity, SongThree: similarity, ...}, userID, {song1: 1.0, song2: 1.0, ...}))  
**Stage Nine:** Aggregate item-item similarity vectors for each target user Based on similarity 
vectors and the target user preference vectors, we can group these two vectors by itemID 
and then generate tuples for further use. We utilise the VectorOrPrefWritable class and 
VectorAndPrefWritable class from the Mahout library to store these kind of tuples.   
Reduce Input: (userID, user-item prefer value × {SongTwo: similarity, SongThree: similarity, ...})  
Reduce Output: (userID, aggregated similarity vector)   
**Stage Ten:** Sort, filter and recommend Also, 
if there are not enough top-10000 recommended songs for a target user,
we append the recommendation list with popular songs.




