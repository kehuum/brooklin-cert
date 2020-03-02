"""This is an example script that demonstrates how to consume a Kafka topic at LinkedIn.
It relies on the 3rd-party library, kafka-python, for Kafka clients. It also documents the
required steps to consume from Kafka in the LinkedIn ecosystem.
"""

from kafka import KafkaConsumer


def main():
    """
    To instantiate a Kafka consumer, you need the following properties:
        + topic:
            - topic name
        + group_id:
            - Consumer group ID
        + auto_offset_reset:
            - Where to start consuming from if no offsets exist for the specified consumer group ID
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
    consumer = KafkaConsumer('test-kafka-push-job-partition-key',  # topic
                             group_id='some-group',
                             auto_offset_reset='earliest',
                             bootstrap_servers=['ltx1-kafka-kafka-dd-local-vip.stg.linkedin.com:16637'],
                             security_protocol="SSL",
                             ssl_check_hostname=False,
                             ssl_cafile='/etc/riddler/ca-bundle.crt',
                             ssl_certfile='identity.pem',
                             ssl_keyfile='identity.pem')
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))


if __name__ == '__main__':
    main()
