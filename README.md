# CC-2020
Code for all the assignments/projest submissions done for UE17CS352 - Cloud Computing



Final project structure contains the 4 folders 
CC_assignment_1
CC_assignment_2
CC_assignment_3
CC

-CC folder is final_project

* Befor running any code in all containers check if any container is already running.  if so u can delete all containers with cmd

	$sudo docker stop $(sudo docker ps -a -q)
	$sudo docker rm $(sudo docker ps -a -q) 

* change IP address in all python file.

CC contain users, rides, CC folders.
* users: contains code for user container.
		 $cd to that folder
		 It has docker-compose.yml file to run
		 just run following command to run the code.
		 
		 $sudo docker-compose up --build 

* rides: contains code for rides container.
		 $cd to that folder
		 It has docker-compose.yml file to run
		 just run following command to run the code.
		 
		 $sudo docker-compose up --build

* CC: contain 2 folders producer and consumer.
		* producer : contains code for orchestrate, zookeeper, scaling 
		* consumer : contains code for DB operations.
		- To run this code.
		    go to that folder. $cd
		    it has docker-compose.yml file to run docker
			Run cmd

			$sudo docker-compose up --build

			/* this cmd will run orchestrate and slave, master container */



