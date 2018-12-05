# Yelp_review_Clustering



### Overview

* In this project, I look at three different clustering algorithms on the Yelp Dataset Challenge Round 12. I clustered the reviews data. For the first problem I implemented K-Means algorithm. For Problems 2 and 3, I run K-Means, Bisecting K-Means and Gaussian Mixture Models based clustering.
* One of the challenges in data mining is preparing the data, extracting features, and presenting results. A significant portion of this assignment focusses on those aspects. The dataset was prepared by extracting the text from reviews in Yelp Challenge Dataset Round 12. All special characters were removed from the reviews by running sub( '\W+' , '' ,word) and then the words were converted to lower case. Only reviews with more than 50 words were selected and one review is written per line. 
* I used word count (only in Problem 1) and Term Frequency – Inverse Document Frequency (TF-IDF) features. The distance measure will be Euclidean Distance. For random seed or state use 20181031.
