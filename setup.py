from setuptools import setup

VERSION = '0.1.0'

setup(
    name='cassandra-s3-incremental-backup-watcher',
    packages=['cassandra_s3_incremental_backup_watcher'],
    version=VERSION,
    description='Continuously end Cassandra incremental backups to Amazon S3',
    long_description=open('README.rst').read(),
    url='https://github.com/Cobliteam/cassandra-s3-incremental-backup-watcher',
    download_url='https://github.com/Cobliteam/cassandra-s3-incremental-backup-watcher/archive/{}.tar.gz'.format(VERSION),
    author='Daniel Miranda',
    author_email='daniel@cobli.co',
    license='MIT',
    install_requires=[
        'boto3',
        'future',
        'watchdog'
    ],
    entry_points={
        'console_scripts': ['cassandra-s3-incremental-backup-watcher=cassandra_s3_incremental_backup_watcher.main:main']
    },
    keywords='cassandra backup incremental-backups aws s3')
