import json
from pathlib import Path

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

from stacks.kafka_ec2_stack import KafkaEc2Stack

CDK_CONTEXT = json.loads((Path(__file__).resolve().parent.parent / "cdk.json").read_text())["context"]


@pytest.fixture(scope="module")
def template():
    app = cdk.App(context=CDK_CONTEXT)
    return Template.from_stack(KafkaEc2Stack(app, "TestStack"))


def single_resource(template, resource_type):
    resources = template.find_resources(resource_type)
    assert len(resources) == 1
    return next(iter(resources.values()))


def test_vpc_has_public_and_private_subnets_with_one_nat_gateway(template):
    template.resource_count_is("AWS::EC2::VPC", 1)
    template.resource_count_is("AWS::EC2::Subnet", 4)
    template.resource_count_is("AWS::EC2::NatGateway", 1)


def test_broker_runs_in_private_subnet(template):
    broker = single_resource(template, "AWS::EC2::Instance")
    assert "private" in broker["Properties"]["SubnetId"]["Ref"].lower()
    assert "NetworkInterfaces" not in broker["Properties"]


def test_broker_has_no_ssh_key(template):
    broker = single_resource(template, "AWS::EC2::Instance")
    assert "KeyName" not in broker["Properties"]


def test_security_group_allows_no_inbound_and_only_https_outbound(template):
    template.resource_count_is("AWS::EC2::SecurityGroupIngress", 0)
    template.has_resource_properties(
        "AWS::EC2::SecurityGroup",
        {
            "GroupDescription": "Kafka broker",
            "SecurityGroupIngress": Match.absent(),
            "SecurityGroupEgress": [
                Match.object_like(
                    {"CidrIp": "0.0.0.0/0", "IpProtocol": "tcp", "FromPort": 443, "ToPort": 443}
                )
            ],
        },
    )


def test_broker_role_only_has_ssm_managed_policy(template):
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "AssumeRolePolicyDocument": Match.object_like(
                {
                    "Statement": [
                        Match.object_like({"Principal": {"Service": "ec2.amazonaws.com"}}),
                    ]
                }
            ),
            "ManagedPolicyArns": [
                {
                    "Fn::Join": [
                        "",
                        Match.array_with([":iam::aws:policy/AmazonSSMManagedInstanceCore"]),
                    ]
                }
            ],
        },
    )


def test_imdsv2_is_required(template):
    launch_template = single_resource(template, "AWS::EC2::LaunchTemplate")
    metadata = launch_template["Properties"]["LaunchTemplateData"]["MetadataOptions"]
    assert metadata["HttpTokens"] == "required"


def test_root_volume_is_encrypted_and_deleted_with_instance(template):
    broker = single_resource(template, "AWS::EC2::Instance")
    ebs = broker["Properties"]["BlockDeviceMappings"][0]["Ebs"]
    assert ebs["Encrypted"] is True
    assert ebs["DeleteOnTermination"] is True
    assert ebs["VolumeType"] == "gp3"


def test_stack_waits_for_bootstrap_signal(template):
    broker = single_resource(template, "AWS::EC2::Instance")
    assert broker["CreationPolicy"]["ResourceSignal"]["Timeout"] == "PT15M"

    user_data = json.dumps(broker["Properties"]["UserData"])
    assert "cfn-signal" in user_data
    assert "sha512sum --check" in user_data


def test_no_resource_is_retained_after_destroy(template):
    resources = template.to_json()["Resources"]
    retained = [
        name
        for name, resource in resources.items()
        if resource.get("DeletionPolicy") in ("Retain", "RetainExceptOnCreate", "Snapshot")
    ]
    assert retained == []
