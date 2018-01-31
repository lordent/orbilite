import importlib
import inspect
import traceback
import asyncio
import ujson
from aio_pika import connect, Message, DeliveryMode, IncomingMessage
import logging

logger = logging.getLogger(__name__)


class Orbilite:

    def __init__(
        self,
        connection_url: str,
        failed_queue_name: str = 'failed',
        exchange_name: str = 'tasks',
    ):
        self.connection_url = connection_url
        self.failed_queue_name = failed_queue_name
        self.exchange_name = exchange_name

    async def create_connection(self):
        return await connect(self.connection_url, loop=asyncio.get_event_loop())

    async def send_message_to_queue(self, channel, queue_name, message_data):
        message = Message(
            ujson.dumps(message_data).encode('utf-8'),
            delivery_mode=DeliveryMode.PERSISTENT
        )

        exchange = await channel.declare_exchange(
            self.exchange_name, auto_delete=True)

        queue = await channel.declare_queue(
            queue_name, durable=True, auto_delete=True)

        await queue.bind(exchange, queue_name)
        await exchange.publish(message, routing_key=queue_name)

    def task(self, queue_name: str,
             requeue_exceptions: tuple, failed_queue_name: str = None):
        def _(func) -> AsyncTask:
            return AsyncTask(
                app=self,
                func=func,
                queue_name=queue_name,
                requeue_exceptions=requeue_exceptions,
                failed_queue_name=failed_queue_name,
            )
        return _

    async def message_process(self, channel, message):
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
                    channel=channel,
                    queue_name=failed_queue,
                    message_data=message_data,
                )

    async def consume_queue(self, queue_name: str):

        async def on_message(message: IncomingMessage):
            await self.message_process(channel=channel, message=message)

        connection = await self.create_connection()

        channel = await connection.channel(
            publisher_confirms=True)
        exchange = await channel.declare_exchange(
            self.exchange_name, auto_delete=True)
        queue = await channel.declare_queue(
            queue_name, durable=True, auto_delete=True)

        await queue.bind(exchange, queue_name)
        await channel.set_qos(prefetch_count=1)

        await queue.consume(on_message)


class AsyncTask:

    def __init__(self, app: Orbilite, func, queue_name: str,
                 requeue_exceptions: tuple, failed_queue_name: str = None):
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

        async with await self.app.create_connection() as connection:
            channel = await connection.channel(publisher_confirms=True)
            await self.app.send_message_to_queue(
                channel=channel,
                queue_name=self.queue_name,
                message_data=message_data
            )
