# Pika Bootstrap

This is a small app intended to bootstrap the pika library if all we want is to read from the amqp queue and forget about the rest of the setup

Just implement `Consumer.__on_event_callback` and away you go.

# Prerequisites

  * Python
  * [Python virtualenv](https://packaging.python.org/guides/installing-using-pip-and-virtualenv/#installing-virtualenv)

# Usage

```
git@github.com:Cisco-AMP/pika_bootstrap.git
cd pika_bootstrap
```

Copy the `stream_template.yml` to `stream.yml` and define the credentials for your AMQP stream

```
cp stream_configs_template.yml stream_configs.yml
vi stream_configs.yml
```

Then, run in a python virtualenv

```
python -m virtualenv env
source env/bin/activate
pip install -r requirements.txt
python consumer.py
```
