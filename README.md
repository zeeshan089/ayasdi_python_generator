Coded in Python Version: 3.5.2
Sqlite3 version: 3.11.0

Requirements

 - External Library used: 

Random_words

Install using pip3: $ sudo pip3 install random_words


- Comment out the sqlite code in the ‘main’ method in the code file and uncomment the CSV writer code just below that. This is for the proof of functionality. The code will write the Tab separated values in a CSV file. Please don’t forget to provide the proper path where you want to save the CSV file.

- Once proof of functionality is established, you can uncomment the sqlite snippet and comment back the CSV writer code. This writes to the sqlite table in memory and not on disk, as writing to temp memory is faster.

- The code execution takes x amount of time to generate all the random data and write to CSV. where 15mins < x < 20mins


P.S: This can be optimized to be much faster by generating data in parts and writing chunks to the table concurrently. I can optimize this and can come up with a faster solution if more time is provided.

