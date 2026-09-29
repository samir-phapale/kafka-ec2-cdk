import shlex
from pathlib import Path

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from constructs import Construct

BOOTSTRAP_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "kafka_bootstrap.sh"

KAFKA_VERSION = "4.1.2"
SCALA_VERSION = "2.13"
KAFKA_SHA512 = (
    "78ac6e488b1071122f9608dfdb363f6fe50e1dbbc492347002c0398dfbf77e0d"
    "8caa5bf794c5937379721004dfe92025937d238b0faf4eb715529417fd43b491"
)


class KafkaEc2Stack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc_cidr: str = "10.0.0.0/16",
        instance_type: str = "t3.medium",
        volume_size: int = 20,
        topic_name: str = "demo-topic",
        topic_partitions: int = 3,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc = ec2.Vpc(
            self,
            "Vpc",
            ip_addresses=ec2.IpAddresses.cidr(vpc_cidr),
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )

        security_group = ec2.SecurityGroup(
            self,
            "BrokerSecurityGroup",
            vpc=vpc,
            description="Kafka broker",
            allow_all_outbound=False,
        )
        security_group.add_egress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(443),
            "HTTPS for packages, Kafka download and SSM",
        )

        role = iam.Role(
            self,
            "BrokerRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"),
            ],
        )

        settings = {
            "KAFKA_VERSION": KAFKA_VERSION,
            "SCALA_VERSION": SCALA_VERSION,
            "KAFKA_SHA512": KAFKA_SHA512,
            "TOPIC_NAME": topic_name,
            "TOPIC_PARTITIONS": str(topic_partitions),
        }

        user_data = ec2.UserData.for_linux()
        user_data.add_commands(
            *(f"export {key}={shlex.quote(value)}" for key, value in settings.items()),
            BOOTSTRAP_SCRIPT.read_text(),
        )

        broker = ec2.Instance(
            self,
            "Broker",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            instance_type=ec2.InstanceType(instance_type),
            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            security_group=security_group,
            role=role,
            user_data=user_data,
            user_data_causes_replacement=True,
            require_imdsv2=True,
            resource_signal_timeout=cdk.Duration.minutes(15),
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        volume_size,
                        volume_type=ec2.EbsDeviceVolumeType.GP3,
                        encrypted=True,
                        delete_on_termination=True,
                    ),
                ),
            ],
        )
        user_data.add_signal_on_exit_command(broker)

        cdk.CfnOutput(self, "InstanceId", value=broker.instance_id)
        cdk.CfnOutput(self, "TopicName", value=topic_name)
