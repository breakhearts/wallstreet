# Wallstreet - Little Weight Nasdaq Crawler

[![Build Status](https://travis-ci.org/breakhearts/wallstreet.svg?branch=master)](https://travis-ci.org/breakhearts/wallstreet)

Wallstreet is a smallest stock deal data collect and analyze system. There are many project aim to crawl and analyze nasdaq stock trading data by wrapping yahoo or nasdaq APIs,
but there're seldom one which provide the complete crawler solution. The ugly work eg. server logging, how to face poor network condition, store date into database may take times to debug.
Wallstreet will provide a lightweight, distributed, incrementally crawler system help you construct your local nasdaq stock database.

## Requirements

- python (2.7+ or 3.5+)
- pip

## Installation

First, install the required python packages:
```bash
sudo pip install -r requirments.txt
```

Then [celery](https://github.com/celery/celery) is used and choose [redis] (http://redis.io/) as default broker:
```base
sudo apt-get install redis
```

Sometimes, you need install pycurl manually in Ubuntu if pip install failed:
```base
sudo apt-get install python-pycurl
```

Later, you need install [mariadb](https://mariadb.org/) or other mysql database, make sure you know how to install it in Ubuntu or you'd better read [Setting up MariaDB Respositories](https://downloads.mariadb.org/mariadb/repositories)

Finally, you need create a database named `wallstreet` to save collected data. 


## How it works

You need start all celery workers as your fist step, [Supervisor](http://supervisord.org/) is a cool process manager and monitor tool can save your time, and you can read [supervisor.conf](supervisor.conf) to learning how to start them manually.

Some configure need be modified in your machine, you need edit `config.json` create a new `config_local.json`(only covers modified fields):
```
{
    "celery":{
      "broker_url": "redis://localhost:6379/0"                          /*redis url as celery broker*/
    },
    "storage":{                                                         
      "db": "sql",
      "url": "mysql+pymysql://root@localhost/wallstreet"                /*mysql url as data storage*/
    },
    "log_server": {                                                     /*logger server if you run workers in more than one manchines, logs will send to central log server*/
      "host": "localhost",                                                      
      "port": 9020
    }
    "edgar":{
      "core_key": "XXXXX"                                              /*edgar api key if you use edgar api*/
    }
}
```

In the root directory, run
```bash
python -m wallstreet.bin.__main__ -h
```
you will see most of the commands available.

And you can use `celery-flower` monitor tasks in http://localhost:5555:
```base
celery -A wallstreet.tasks flower
```
