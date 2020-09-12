# kafka-mass-produce.py

## Purpose
A utility for mass-producing data to Kafka clusters.

## How to use
1. Clone `brooklin-certification` and activate your Python virtual environment as 
prescribed on the [README](../README.md) page.

2. Navigate to the `src` directory and generate a Grestin certificate for your LDAP user account by running:
   ```shell script
   cd src
   ../gen-cert.sh 
   ```
   
3. Run the utility, e.g.
    ```shell script   
    (brooklin-certification) bash-3.2$ python kafka-mass-produce.py --bs kafka.qei.kafka.ei-ltx1.atd.stg.linkedin.com:16637 -t brooklin-perf-16-dragon -p 1 --random
    ```

## Miscellaneous
- Make sure your LDAP user account has permissions to produce to the Kafka topics you specify ([go/aclin](https://aclin.nuage.prod.linkedin.com/)).
- You may consult [go/kcd](https://aclin.nuage.prod.linkedin.com/) to find out the bootstrap servers URLs of the different Kafka clusters.
