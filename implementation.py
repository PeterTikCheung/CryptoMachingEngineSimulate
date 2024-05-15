from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
from order_book import OrderBook
from order import Order
from kafka.admin import KafkaAdminClient, NewTopic
order_book = OrderBook()
def process_order(order):
    print(f"Instrument: {order.instrument}, Quantity: {order.quantity}, Price: {order.price}, Order ID: {order.order_id}")
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(2,0,2),
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))

    # Add the order to the order book
    order_book.add_order(order)

    # Get the buy and sell orders for the instrument in the order book
    buys = order_book.get_orders(order.instrument)[order.price]['buy']
    sells = order_book.get_orders(order.instrument)[order.price]['sell']

    # If there are buy and sell orders for the instrument at the given price, match them and print the trade details
    if len(buys) > 0 and len(sells) > 0:
        trade = order_book.match_orders(buys[0], sells[0])
        buys = order_book.get_orders(order.instrument)[order.price]['buy']
        sells = order_book.get_orders(order.instrument)[order.price]['sell']
        if trade is not None:
            trade_message = {"instrument": trade.instrument, "quantity": trade.quantity, "price": trade.price,
                             "buy_order_id": trade.buy_order_id, "sell_order_id": trade.sell_order_id}
            # Send the trade message to the output queue
            producer.send('trades', value=trade_message)
            print("Trade executed:", trade.quantity, trade.instrument, "at price", trade.price, "between orders",
                  trade.buy_order_id, "and", trade.sell_order_id)
            # Check if there are remaining buy orders
            while len(buys) > 0 and len(sells) > 0:
                trade = order_book.match_orders(buys[0], sells[0])
                buys = order_book.get_orders(order.instrument)[order.price]['buy']
                sells = order_book.get_orders(order.instrument)[order.price]['sell']
                if trade is not None:
                    trade_message = {"instrument": trade.instrument, "quantity": trade.quantity, "price": trade.price,
                                     "buy_order_id": trade.buy_order_id, "sell_order_id": trade.sell_order_id}
                    # Send the trade message to the output queue
                    producer.send('trades', value=trade_message)
                    print("Trade executed:", trade.quantity, trade.instrument, "at price", trade.price, "between orders",
                          trade.buy_order_id, "and", trade.sell_order_id)
                    print(order_book.orders)

        # Send an order acknowledgment to the output queue
    ack_message = {"order_id": order.order_id}
    producer.send('acknowledgments', value=ack_message)

def place_order(order):
    # Create a Kafka producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(2,0,2),
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))
    admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'], api_version=(2,0,2))

    # Check if the topic 'orders' exists
    topic_exists = False
    for topic in admin_client.list_topics():
        if topic == 'orders':
            topic_exists = True
            break

    # If the topic does not exist, create it
    if not topic_exists:
        new_topic = NewTopic(name='orders', num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[new_topic])
    # Serialize the order as JSON
    order_data = {
        'instrument': order.instrument,
        'quantity': order.quantity,
        'price': order.price,
        'order_id': order.order_id
    }
    # Publish the order to the Kafka topic
    producer.send('orders', value=order_data)

    # Wait for any outstanding messages to be delivered and delivery reports received
    producer.flush()

    print(f"Order {order.order_id} has been placed.")

def get_order_book():
    # Return the current state of the order book
    return order_book.orders


def get_executed_trades():
    # Create a Kafka consumer instance
    consumer = KafkaConsumer('trades', bootstrap_servers=['localhost:9092'], api_version=(2, 0, 2),
                             value_deserializer=lambda m: json.loads(m.decode('ascii')),  auto_offset_reset='earliest')
    trades = []
    for message in consumer:
        trade_data = message.value
        trades.append(trade_data)
        if message.offset == consumer.end_offsets([TopicPartition(message.topic, message.partition)])[TopicPartition(message.topic, message.partition)] - 1:
            break

    return trades

def cancel_order(order_id):
    for instrument in order_book.orders:
        for price in order_book.orders[instrument]:
            for order in order_book.orders[instrument][price]:
                if order == 'buy':
                    for buyOrder in order_book.orders[instrument][price][order]:
                        order_book.remove_order(buyOrder, True)
                        return {"message": f"Order {order_id} canceled successfully."}
                else:
                    for sellOrder in order_book.orders[instrument][price][order]:
                        order_book.remove_order(sellOrder, False)
                        return {"message": f"Order {order_id} canceled successfully."}

    # Return an error message if the order with the specified order_id was not found in the order book
    return {"message": f"Order {order_id} not found."}

def consume_orders(consumer):
    for message in consumer:
        order_data = message.value
        order = Order(instrument=order_data['instrument'], quantity=order_data['quantity'], price=order_data['price'],
                      order_id=order_data['order_id'])
        process_order(order)

def consume_acknowledgments(consumer):
    for message in consumer:
        ack_data = message.value
        print("order acknowledgments: " + str(ack_data['order_id']))
