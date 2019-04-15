# realtime-crypto-analysis
Final Project for CMPT-733@SFU

In this project we have created a platform for real-time cryptocurrency prediction. 
The platform received the news and price history as its input, and it performs feature extraction, feature aggregation, 
and price movement prediction. Finally, the platform outputs the predicted Bitcoin price movement for next minute. 
At each stage in the pipeline the data is read from a Kafka and the new data is written into another Kafka. 
