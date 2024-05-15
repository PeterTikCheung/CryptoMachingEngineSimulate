from trade import Trade

class OrderBook:
    def __init__(self):
        self.orders = {}

    def add_order(self, order):
        if order.instrument not in self.orders:
            self.orders[order.instrument] = {}
        if order.price not in self.orders[order.instrument]:
            self.orders[order.instrument][order.price] = {'buy': [], 'sell': []}
        if order.quantity > 0:
            self.orders[order.instrument][order.price]['buy'].append(order)
        elif order.quantity < 0:
            self.orders[order.instrument][order.price]['sell'].append(order)
        print(self.orders)
    def get_orders(self, instrument):
        if instrument not in self.orders:
            return {}
        return self.orders[instrument]

    def remove_order(self, order, isBuy):
        if order.instrument in self.orders and order.price in self.orders[order.instrument]:
            if isBuy:
                self.orders[order.instrument][order.price]['buy'].remove(order)
            else:
                self.orders[order.instrument][order.price]['sell'].remove(order)


    def match_orders(self, buy_order, sell_order):
        trade_quantity = min(abs(buy_order.quantity), abs(sell_order.quantity))
        trade_price = sell_order.price
        buy_order.quantity -= trade_quantity
        sell_order.quantity += trade_quantity
        if buy_order.quantity == 0:
            self.remove_order(buy_order, True)
        if sell_order.quantity == 0:
            self.remove_order(sell_order, False)
        return Trade(buy_order.instrument, trade_quantity, trade_price, buy_order.order_id, sell_order.order_id)


