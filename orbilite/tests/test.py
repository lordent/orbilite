import os
import asyncio
import pytest
from aio_pika import ExchangeType, \
    Channel, IncomingMessage, exceptions

from orbilite import Orbilite, AsyncTask


QUEUE_NAME = 'orbilite_test_queue'
FAILED_QUEUE_NAME = 'orbilite_test_failed_queue'
EXCHANGE_NAME = 'orbilite_test_tasks'
EXCHANGE_TYPE = ExchangeType.FANOUT

MESSAGES_AMOUNT = 10000

counters = {
    'successed': 0,
    'requeued': 0,
    'failed': 0,
}

requeued = list()

task_app = Orbilite(
    connection_url=os.getenv(
        'RABBITMQ_URL', 'amqp://guest:guest@localhost'),
    exchange_name=EXCHANGE_NAME,
    exchange_type=EXCHANGE_TYPE,
    failed_queue_name=FAILED_QUEUE_NAME,
)


class RequeueException(Exception):
    pass


class FailException(Exception):
    pass


@task_app.task(queue_name=QUEUE_NAME,
               requeue_exceptions=(RequeueException, ))
async def worker(message):

    if not message['id'] % 20:
        counters['failed'] += 1
        raise FailException()

    if not message['id'] % 10 and message['id'] not in requeued:
        counters['requeued'] += 1
        requeued.append(message['id'])
        raise RequeueException()

    counters['successed'] += 1


@pytest.yield_fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session')
def app():
    return task_app


@pytest.mark.asyncio(scope='session')
async def test_worker():
    assert isinstance(worker, AsyncTask) is True

    with pytest.raises(FailException):
        await worker.func({
            'foo': 'bar',
            'id': 20,
        })

    assert counters['failed'] == 1
    counters['failed'] = 0

    with pytest.raises(RequeueException):
        await worker.func({
            'foo': 'bar',
            'id': 10,
        })

    assert counters['requeued'] == 1
    counters['requeued'] = 0
    requeued.clear()


@pytest.mark.asyncio(scope='session')
async def test_init(app):
    channel = await app.producer()
    assert isinstance(channel, Channel) is True

    exchange = await channel.declare_exchange(
        name=EXCHANGE_NAME,
        type=EXCHANGE_TYPE,
        durable=True,
    )

    queue = await channel.declare_queue(
        QUEUE_NAME, durable=True)
    await queue.bind(exchange, QUEUE_NAME)

    await channel.declare_queue(
        FAILED_QUEUE_NAME, durable=True)


@pytest.mark.asyncio(scope='session')
async def test_producer():
    for i in range(0, MESSAGES_AMOUNT):
        message = {
            'foo': 'bar',
            'id': i,
        }
        await worker(message)


@pytest.mark.asyncio(scope='session')
async def test_consumer(app):
    channel = await app.consumer()
    await channel.set_qos(prefetch_count=1)

    exchange = channel.default_exchange

    queue = await channel.declare_queue(QUEUE_NAME, durable=True)

    while True:

        try:
            message = await queue.get(timeout=5)
        except exceptions.QueueEmpty:
            break

        assert isinstance(message, IncomingMessage) is True

        with message.process(ignore_processed=True):
            await app.message_process(
                exchange=exchange,
                message=message,
            )


@pytest.mark.asyncio(scope='session')
async def test_delete(app):
    channel = await app.producer()

    await channel.queue_delete(QUEUE_NAME)
    await channel.queue_delete(FAILED_QUEUE_NAME)
    await channel.exchange_delete(EXCHANGE_NAME)


@pytest.mark.asyncio(scope='session')
async def test_check():
    assert counters['failed'] == int(MESSAGES_AMOUNT / 20)
    assert counters['successed'] == MESSAGES_AMOUNT - counters['failed']
    assert counters['requeued'] == (
        int(MESSAGES_AMOUNT / 10) - counters['failed']
    )
