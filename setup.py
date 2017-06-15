from setuptools import setup, find_packages

# Dependencies for using the library:
dependencies = [
    'pika',
    'requests'
]

# Dependencies for running the tests:
test_dependencies = [
    'mock'
]

# Packages to be included for using the library:
packages = [
    'esgfpid',
    'esgfpid/assistant',
    'esgfpid/utils',
    'esgfpid/rabbit',
    'esgfpid/rabbit/synchronous',
    'esgfpid/rabbit/asynchronous',
    'esgfpid/solr',
    'esgfpid/solr/tasks'
]

# Packages to be included for running the tests:
test_packages = [
    'tests',
    'tests/resources',
    'tests/testcases',
    'tests/testcases/solr',
    'tests/testcases/rabbit',
    'tests/testcases/rabbit/syn',
    'tests/testcases/rabbit/asyn',
    'tests/utils'
]

# Load description from README.md
# Note: The package maintainer needs pypandoc and pygments to properly convert
# the Markdown-formatted README into RestructuredText before uploading to PyPi
# See https://bitbucket.org/pypa/pypi/issues/148/support-markdown-for-readmes
try:
    import pypandoc
    long_description=pypandoc.convert('README.md', 'rst')
except(IOError, ImportError):
    long_description=open('README.md').read()


setup(
    name='esgfpid',
    version='0.7.8',
    author='Merret Buurman, German Climate Computing Centre (DKRZ)',
    author_email='buurman@dkrz.de',
    url='https://github.com/IS-ENES-Data/esgf-pid',
    download_url='https://github.com/IS-ENES-Data/esgf-pid/archive/0.7.8.tar.gz',
    description='Library for sending PID requests to a rabbit messaging queue during ESGF publication.',
    long_description=long_description,
    packages=packages + test_packages,
    install_requires=dependencies,
    tests_require=test_dependencies,
    include_package_data=True,
    classifiers=[
       'Development Status :: 4 - Beta',
       'Programming Language :: Python :: 2',
       'Programming Language :: Python :: 2.7',
       'License :: OSI Approved :: Apache Software License',
       'Intended Audience :: Developers',
       'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    zip_safe=False
)