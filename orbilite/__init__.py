import importlib
import inspect
import traceback
import asyncio
import ujson
from aio_pika import connect, Connection, Channel, ExchangeType, \
    Message, DeliveryMode, IncomingMessage
import logging

logger = logging.getLogger(__name__)


class Orbilite:

    def __init__(
        self,
        connection_url: str,
        producer_url: str = None,
        consumer_url: str = None,
        failed_queue_name: str = 'failed',
        exchange_name: str = 'tasks',
        exchange_type: ExchangeType = ExchangeType.DIRECT,
    ):
        self.producer_url = producer_url or connection_url
        self.consumer_url = consumer_url or connection_url

        self.failed_queue_name = failed_queue_name
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type

        self._producer = None
        self._consumer = None

    @staticmethod
    async def create_connection(url) -> Connection:
        return await connect(url, loop=asyncio.get_event_loop())

    @staticmethod
    async def channel_connection(connection: Connection, **kwargs) -> Channel:
        return await connection.channel(**kwargs)

    async def producer(self) -> Channel:
        self._producer = self._producer or \
                         await self.channel_connection(
                             await self.create_connection(
                                 self.producer_url)
                         )
        return self._producer

    async def consumer(self) -> Channel:
        self._consumer = self._consumer or \
                         await self.channel_connection(
                            await self.create_connection(
                                self.consumer_url),
                            publisher_confirms=True
                         )
        return self._consumer

    @staticmethod
    async def send_message_to_queue(exchange, queue_name, message_data):
        await exchange.publish(
            Message(
                ujson.dumps(message_data).encode('utf-8'),
                content_type='application/json',
                delivery_mode=DeliveryMode.PERSISTENT
            ), routing_key=queue_name
        )

    def task(self, queue_name: str,
             requeue_exceptions: tuple = None, failed_queue_name: str = None):
        def _(func) -> AsyncTask:
            return AsyncTask(
                app=self,
                func=func,
                queue_name=queue_name,
                requeue_exceptions=requeue_exceptions,
                failed_queue_name=failed_queue_name,
            )
        return _

    async def message_process(self, exchange, message):
        with message.process(ignore_processed=True):
            message_data = ujson.loads(message.body.decode('utf-8'))
            function_module, function_name = message_data['function']

            requeue_exceptions = ()
            failed_queue = self.failed_queue_name

            try:
                async_task = getattr(
                    importlib.import_module(function_module),
                    function_name
                )

                requeue_exceptions = async_task.requeue_exceptions
                failed_queue = async_task.failed_queue_name

                await async_task.func(
                    *message_data['args'],
                    **message_data['kwargs'],
                )
            except requeue_exceptions as e:
                logger.warning(str(e))

                message.reject(requeue=True)
            except Exception as e:
                logger.error(e, exc_info=True)

                message_data['error_message'] = traceback.format_exc()

                await self.send_message_to_queue(
                    exchange=exchange,
                    queue_name=failed_queue,
                    message_data=message_data,
                )

    async def consume_queue(self, queue_name: str):
        async def on_message(message: IncomingMessage):
            await self.message_process(exchange=exchange, message=message)

        channel = await self.consumer()
        await channel.set_qos(prefetch_count=1)

        exchange = channel.default_exchange

        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.consume(on_message)


class AsyncTask:

    def __init__(self, app: Orbilite, func, queue_name: str,
                 requeue_exceptions: tuple = None,
                 failed_queue_name: str = None):
        self.app = app
        self.queue_name = queue_name
        self.func = func
        self.requeue_exceptions = requeue_exceptions or ()
        self.failed_queue_name = failed_queue_name or app.failed_queue_name

    async def __call__(self, *args, **kwargs):
        message_data = {
            'function': (
                inspect.getmodule(self.func).__name__,
                self.func.__name__,
            ),
            'args': args,
            'kwargs': kwargs,
        }

        channel = await self.app.producer()
        exchange = await channel.declare_exchange(
            self.app.exchange_name, type=self.app.exchange_type, durable=True)

        await self.app.send_message_to_queue(
            exchange=exchange,
            queue_name=self.queue_name,
            message_data=message_data
        )
