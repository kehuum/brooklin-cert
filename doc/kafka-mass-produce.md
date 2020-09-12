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


## Using Python 3.6 on Linux

1. `git clone git@github.com:sutambe/brooklin-certification.git`
2. `mkdir test`
3. `cd test`
4. `virtualenv -p /usr/bin/python3.6 venv`
5. `source venv/bin/activate`
6. `pip install kafka requests`
7. `id-tool grestin sign`
8. `cat identity.key identity.cert > identity.pem`
9. `python ../brooklin-certification/src/kafka-mass-produce.py`
