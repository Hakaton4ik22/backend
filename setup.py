from setuptools import setup

setup(
    name='app-backend',
    version='0.0.1',
    author='Daniil K',
    author_email='dreamteam',
    description='FastApi app',
    install_requires=[
        'fastapi==0.70.0',
        'uvicorn==0.15.0',
        'SQLAlchemy==1.4.26',
        'psycopg2',
        'pytest==6.2.5',
        'requests==2.26.0',
        'pandas'
    ],
    scripts=[
        'app/main.py'
    ]
)
