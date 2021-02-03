## Send Twitter feed to Kafka

1. Make sure you have right Python libs installed 
   - kafka can be installed by : 
   ```
   conda install kafka-python
   ```
1. Edit the docker-compose.yml to have the current IP of your machine 
1. Run the docker images using
   ```
   docker-compose up 
   ```
1. Once you see messages like "creatin topics: twitter:1:1", you can run the python script
1. Edit subscribe.py to set the correct Twitter API token, secret etc...
1. Run the script 
    ```
    python subscribe.py 
    ```
1. The twitter text should be available on the "twitter" topic


