from setuptools import setup


setup(
    name='orbilite',
    version='1.0.1',
    description='asyncio task manager',
    long_description='',
    author='Vitaliy Nefyodov',
    author_email='vitent@gmail.com',
    packages=['orbilite'],
    url='https://github.com/lordent/orbilite',
    install_requires=[
        'ujson',
        'aio_pika',
    ],
    extras_require={
        'develop': [
            'pylama',
            'pytest',
            'pytest-asyncio',
            'tox',
        ],
    },
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
    ]
)
