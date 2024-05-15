class Trade:
    def __init__(self, instrument, quantity, price, buy_order_id, sell_order_id):
        self.instrument = instrument
        self.quantity = quantity
        self.price = price
        self.buy_order_id = buy_order_id
        self.sell_order_id = sell_order_id