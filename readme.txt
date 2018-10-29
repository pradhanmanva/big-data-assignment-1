Input Files:
	1. userdata.txt - contains the user details <user_id, user_data>
	2. soc_file.txt - contains the user and user's friends <user_id, list_of_friends>

Part 1 : (/home/011/m/mx/mxp174430/hadoop_homework/mutual_friends) 
	1. Keep the folder as it is.
	2. Usage : hadoop jar mutual_friends.jar mutualFriends /manva/input1/soc_file.txt/manva.output1
	3. Output : /manva/output1/
	4. In the source code folder mutual_friend (attached in the zip), the folder mutual_friends_output contains screenshots and part-r-00000 (output).
	5. Seeing output : hdfs dfs -cat /manva/output1/part-r-00000 | grep "<user1_id_>,<user2_id>      " (Tab using ctrl+v and TAB)

Part 2 : (/home/011/m/mx/mxp174430/hadoop_homework/top_ten_friends)
	1. Keep the folder as it is.
	2. Usage : hadoop jar top_ten_friends.jar topten /manva/input1/soc_file.txt /manva/output2.1 /manva/output2.2
	3. Seeing the top 10 friends : hdfs dfs -cat /manva/output2.2/part-r-00000
	4. In the source code folder top_ten_friends (attached in the zip), the folder top_ten_friends_output contains screenshots and part-r-00000 (output).

Part 3 : (/home/011/m/mx/mxp174430/hadoop_homework/inmemory_join)
	1. Keep the folder as it is.
	2. Usage : inmemory_join <user_1> <user_2> <in_path> <user_data_path> <user_data_output_path>
	hadoop jar inmemory_join.jar inmemory_join <user1_id> <user2_id> /manva/input1/soc_file.txt /manva/input1/userdata.txt /manva/output3
	3. Seeing the output : hdfs dfs -cat /manva/output3/part-r-0000
	4. In the source code folder inmemory_join (attached in the zip), the folder inmemory_join_output contains screenshots and part-r-00000 (output). 

Part 4 : (/home/011/m/mx/mxp174430/hadoop_homework/reduce_side_join)
	1. Keep the folder as it is.
	2. Usage : reduce_side_join <in_path> <user_data_path> <user_data_output_path> <output_path>
	hadoop jar reduce_side_join.jar /manva/input1/soc_file.txt /manva/input1/userdata.txt /manva/output4.1 /manva/output4.2
	3. Seeing the output : hdfs dfs -cat /manva/output4.2/part-r-00000
	4. In the source code folder reduce_side_join (attached in the zip), the folder reduce_side_join_output contains screenshots and part-r-00000 (output). 
