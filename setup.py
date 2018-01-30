from setuptools import setup


setup(
    name='orbilite',
    version='1.0.0',
    description='asyncio task manager',
    long_description='',
    author='Vitaliy Nefyodov',
    author_email='vitent@gmail.com',
    packages=[''],
    url='https://github.com/lordent/orbilite',
    requires=[
        'ujson',
        'aio_pika',
    ],
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
    ]
)
