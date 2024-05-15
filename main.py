from implementation import *
import time
import subprocess
import os, stat, sys
import threading
import platform

kafka_dir = "kafka_2.12-3.4.0"
if platform.system() == "Windows":
    zookeeper_cmd = ["cmd.exe", "/c", "start", ".\\bin\\windows\\zookeeper-server-start.bat", ".\\config\\zookeeper.properties"]
    kafka_cmd = ["cmd.exe", "/c", "start", ".\\bin\\windows\\kafka-server-start.bat", ".\\config\\server.properties"]
    zookeeper_proc = subprocess.Popen(zookeeper_cmd, cwd=kafka_dir)
    time.sleep(2)
    # Start Kafka
    kafka_proc = subprocess.Popen(kafka_cmd, cwd=kafka_dir)
    time.sleep(5)

elif platform.system() == "Linux":
    # Add execute permission to the shell scripts
    os.chmod(os.path.join(kafka_dir, "bin", "zookeeper-server-start.sh"), stat.S_IRWXU)
    os.chmod(os.path.join(kafka_dir, "bin", "kafka-server-start.sh"), stat.S_IRWXU)
    zookeeper_cmd = ["gnome-terminal", "--", "./bin/zookeeper-server-start.sh", "config/zookeeper.properties"]
    subprocess.Popen(zookeeper_cmd, cwd=os.path.join(kafka_dir, ""))
    time.sleep(2)
    # Start Kafka
    kafka_cmd = ["gnome-terminal", "--", "./bin/kafka-server-start.sh", "config/server.properties"]
    subprocess.Popen(kafka_cmd, cwd=os.path.join(kafka_dir, ""))
    time.sleep(2)
else:
    print("Unsupported platform.")



# Create Kafka consumers
consumer = KafkaConsumer('orders', bootstrap_servers=['localhost:9092'], api_version=(2, 0, 2),
                         value_deserializer=lambda m: json.loads(m.decode('ascii')))

ackConsumer = KafkaConsumer('acknowledgments', bootstrap_servers=['localhost:9092'], api_version=(2, 0, 2),
                            value_deserializer=lambda m: json.loads(m.decode('ascii')))


def consume_orders_thread():
    while True:
        print("\nStarting to consume orders...")
        consume_orders(consumer)

t = threading.Thread(target=consume_orders_thread)
t.daemon = True
t.start()

def consume_acknowledgments_thread():
    while True:
        print("\nStarting to consume acknowledgments...")
        consume_acknowledgments(ackConsumer)

t2 = threading.Thread(target=consume_acknowledgments_thread)
t2.daemon = True
t2.start()

time.sleep(1)
while True:
    print("Choose an action:")
    print("1. Place a new buy or sell order with a specified instrument and quantity at a given price.")
    print("2. Retrieve the current order book for a specific instrument.")
    print("3. Retrieve the executed trades for a specific instrument.")
    print("4. Cancel an existing order.")
    print("5. Exit")
    choice = input("Enter your choice: ")

    if choice == "1":
        instrument = input("Enter the instrument: ")
        quantity = int(input("Enter the quantity: "))
        price = float(input("Enter the price: "))
        order_id = int(time.time())
        order_type = input("Enter the order type (buy/sell): ")
        if order_type == "buy":
            quantity = abs(quantity)
        elif order_type == "sell":
            quantity = -abs(quantity)
        else:
            print("Invalid order type.")
            continue
        order = Order(instrument=instrument, quantity=quantity, price=price, order_id=order_id)
        place_order(order)
        time.sleep(2)




    elif choice == "2":
        instrument = input("Enter the instrument: ")
        orders = get_order_book().get(instrument, {})
        print(f"{'Price':<10}{'Order id':<15}{'Buy':<15}{'Sell':<15}")
        for price in sorted(orders.keys()):
            buys = [(order.order_id, order.quantity) for order in orders[price]['buy']]
            sells = [(order.order_id, abs(order.quantity)) for order in orders[price]['sell']]
            max_len = max(len(buys), len(sells))
            for i in range(max_len):
                if i < len(buys):
                    buy_id, buy_qty = buys[i]
                    print(f"{price:<10}{buy_id:<15}{buy_qty:<15}\n", end='')
                if i < len(sells):
                    sell_id, sell_qty = sells[i]
                    print(f"{price:<10}{sell_id:<30}{sell_qty:<15}\n", end='')
        time.sleep(2)

    elif choice == "3":
        instrument = input("Enter the instrument: ")
        trades = get_executed_trades()

        trades = [t for t in trades if t['instrument'] == instrument]
        if not trades:
            print("No executed trades found.")
        else:
            print(f"{'Price':<10}{'Quantity':<15}{'Buy Order ID':<15}Sell Order ID")
            for trade in trades:
                print(f"{trade['price']:<10}{trade['quantity']:<15}{trade['buy_order_id']:<15}{trade['sell_order_id']}")
        time.sleep(2)
    elif choice == "4":
        order_id = int(input("Enter the order ID: "))
        cancel_order(order_id)
        print(f"Order {order_id} has been canceled.")
        time.sleep(2)
    elif choice == "5":
        print("Exiting...")
        break

    else:
        print("Invalid choice.")