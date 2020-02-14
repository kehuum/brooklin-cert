import setuptools

setuptools.setup(
    name="brooklin-certification",
    version="0.0.1",
    packages=setuptools.find_packages(),
    scripts=["samples/kafka-consume.py",
             "samples/kafka-produce.py",
             "tools/kafka/kafka-audit-v2.py"],
    data_files=[("data", ["data/topics.txt"])],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
