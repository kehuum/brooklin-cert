import glob
import setuptools


def get_files(*dirs, ext='*'):
    scripts = []
    for d in dirs:
        scripts += glob.glob(f'{d}/*.{ext}')
    return scripts


setuptools.setup(
    name="brooklin-certification",
    version="0.0.2",
    packages=setuptools.find_packages(),
    scripts=get_files(".", "samples", ext="py"),
    data_files=[("data", get_files("data", ext="txt"))],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
