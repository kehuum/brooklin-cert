"""This is an example script that demonstrates how to use a Kafka admin client at LinkedIn.
It relies on the 3rd-party library, kafka-python.
"""

from kafka import KafkaAdminClient
from kafka.admin import NewTopic


def main():
    """
    To instantiate a Kafka admin client, you need the following properties:
        + bootstrap_servers:
            - This can be retrieved from go/kcd.
            - Make sure to use the SSL port (16637)
        + security_protocol: "SSL"
            - We are communicating with Kafka over SSL/TLS
        + ssl_check_hostname: False
            - No need to have the SSL handshake verify that certificates match brokers hostname
        + ssl_cafile: /etc/riddler/ca-bundle.crt
            - Certificate authority. We use Riddler's signed certificate present on every host at LinkedIn.
        + ssl_certfile:
            - Client certificate in the pem file format.
            - Can be generated using the instructions at: go/kafka-gen-cert
            - You can use your own user's certificate (e.g. > id-tool grestin sign -u $USER)
            - You can also use the tester's account certificate (e.g. id-tool grestin sign-tester)
        + ssl_keyfile:
            - Client private key. Can be the same pem file specified for ssl_certfile
    """
    admin = KafkaAdminClient(bootstrap_servers=['kafka.charlie.kafka.ei-ltx1.atd.stg.linkedin.com:16637'],
                             security_protocol="SSL",
                             ssl_check_hostname=False,
                             ssl_cafile='/etc/riddler/ca-bundle.crt',
                             ssl_certfile='identity.pem',
                             ssl_keyfile='identity.pem')

    admin.create_topics([NewTopic(name='dummy-topic', num_partitions=10, replication_factor=3)])
    admin.delete_topics(topics=['dummy-topic'])


if __name__ == '__main__':
    main()
