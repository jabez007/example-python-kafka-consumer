import os


class ConsumerConfig:

    def __init__(self):
        self.bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", localhost:9092
        )
        self.group_id = os.getenv("KAFKA_GROUP_ID", my_consumer_group)
        self.auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
        self.auto_commit_offset = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false")
