#  Orbilite: Async task manager on RabbitMQ queue and aio_pika package

## aio_pika Documentation

Full documentation is available at https://aio-pika.readthedocs.io/en/latest/ .

## Usage examples

### Create manager instance
```python
from orbilite import Orbilite


task_manager = Orbilite('amqp://localhost/')
```

### Decorate function as task
```python
import asyncio


@task_manager.task(queue_name='main')
async def sleep_function(seconds):
    await asyncio.sleep(seconds)
```

### Call decorated function
```python
import asyncio

loop = asyncio.get_event_loop()


async def main():
    await sleep_function(1)


if __name__ == '__main__':
    loop.create_task(main())
    loop.run_forever()
```

### Consume queue
```python
import asyncio

loop = asyncio.get_event_loop()


async def main():
    await task_manager.consume_queue('main')


if __name__ == '__main__':
    loop.create_task(main())
    loop.run_forever()
```

## Requirements
- Python >= 3.6
