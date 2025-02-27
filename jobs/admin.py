import logging

from confluent_kafka.admin import (
    AdminClient, 
    NewTopic,
    NewPartitions)

from typing import *

class KafkaAdmin:

    '''Admin class for managing Kafka topics.'''

    def __init__(self,bootstrap_server) -> None:

        logging.basicConfig(level=logging.INFO)

        self.logger = logging.getLogger(__class__.__name__)

        self.bootstrap_server = bootstrap_server

        self.admin = AdminClient({'bootstrap.servers': self.bootstrap_server})

    def topic_exists(self,topic):
        '''Check topic exists'''
        all_topics = self.admin.list_topics()
        return topic in all_topics.topics.keys()

    def create_topic(self,topic_name:str,num_partitoons:int,replication_factor:int) -> Dict:
        
        try:
            if not self.topic_exists(topic_name):

                # replica_assignment=[[1,2],[0,2],[0,1]]
                new_topic = NewTopic(
                                topic_name,
                                num_partitions = num_partitoons,
                                replication_factor=replication_factor
                            )

                return_val = self.admin.create_topics([new_topic])

                self.logger.info(f'Topic {topic_name} has been created {return_val}')

            else:
                self.logger.info(f'Topic {topic_name} already exists...')

        except Exception as exe:
            self.logger.error(exe)
            raise exe





