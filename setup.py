# module load anaconda/5.2.0-py3.6
# ijob -c 4 --mem-per-cpu=36000 -A netsec -p dev --time=01:00:00
# pip install .[dev] -t /home/$USER/local_pip
# python test_file.py

# Use the following to command to run the pytests in the test directory itself:
# pip install -e .
from setuptools import setup, find_packages, Extension
import numpy

setup(
    name='generate_features',
    version="1.0",
    author="pcore",
    author_email="atn5vs@virginia.edu",
    description="compute all the features",
    long_description_content_type="text/markdown",
    url="https://code.vt.edu/p-core/features",
    packages=find_packages(),
    include_package_data=True,
    python_requires='>=3.6',
    extras_require={
        'dev': [
            #'wheel',
            #'pytest',
            'pyspark',
            'msgpack',
            'pyarrow',
            'fastparquet',
            'pandas',
            'numpy',
            'scipy',
            'tldextract',
            'ua_parser',
            'click',
            'netaddr',
            'slurmpy',
            #'mdv',
            'fasteners',
            'python-dateutil',
            'psutil',
            'schedule',
        ]
    },
    include_dirs=[numpy.get_include()],
    install_requires=[
        'click',
    ],
    entry_points='''
        [console_scripts]
        features=generate_features.cli:cli
    ''',
)
