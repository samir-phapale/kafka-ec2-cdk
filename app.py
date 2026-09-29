import aws_cdk as cdk

from stacks.kafka_ec2_stack import KafkaEc2Stack

app = cdk.App()

KafkaEc2Stack(app, "KafkaEc2Stack")

cdk.Tags.of(app).add("Project", "kafka-ec2-cdk")

app.synth()
