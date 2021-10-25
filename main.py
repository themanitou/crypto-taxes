# Calculate net gains/losses based on Swyftx crypto trading log file
# Author: Quan Nguyen (https://github.com/themanitou)

import argparse
import re
import pandas as pd
import heapq
from datetime import datetime

class _RegExLib:
    """Set up regular expressions"""
    _reg_market = re.compile(r'^Market (?P<action>(Buy|Sell))\t(?P<amount>([0-9]*[.])?[0-9]+) (?P<asset>[A-Z]+)')
    _reg_quantity = re.compile(r'(?P<amount>([0-9]*[.])?[0-9]+) (?P<unit>[A-Z/]+)')
    _reg_date = re.compile(r'^Completed\t(?P<date>[ APM:0-9/]+)')

    # use __slots__ to help with memory and performance
    __slots__ = ['market', 'quantity', 'date']

    def __init__(self, line):
        # check whether line has a positive match with all of the regular expressions
        self.market = self._reg_market.match(line)
        self.quantity = self._reg_quantity.match(line)
        self.date = self._reg_date.match(line)

def process(data):
    data.apply(pd.to_numeric, errors='ignore')
    assets = data["asset"].unique()
    data.set_index(['asset'], inplace=True)
    data = data.sort_index()
    data = data.sort_values(by=['date', 'action'])

    profits = {}
    for asset in assets:
        asset_data = data.loc[asset]
        print(f'{ asset = }, { len(asset_data.shape) = }, {asset_data = }')

        if len(asset_data.shape) == 1:
            continue

        max_heap = []
        heapq.heapify(max_heap)
        for row in range(asset_data.shape[0]):
            action = asset_data.iloc[row]["action"]
            amount = float(asset_data.iloc[row]["amount"])
            price = float(asset_data.iloc[row]["price"])

            if action == 'Buy':
                print(f' --- Bought { amount = } at { price = }')
                heapq.heappush(max_heap, (-price, amount))

            else:    # action == 'Sell'
                print(f' --- Sold { amount = } at { price = }')
                acc_amount_bought = 0.0
                acc_total_bought = 0.0

                while (acc_amount_bought < amount) and (len(max_heap) > 0):
                    price_bought, amount_bought = heapq.heappop(max_heap)
                    price_bought = -price_bought
                    amount_needed = min(amount - acc_amount_bought, amount_bought)
                    acc_total_bought += price_bought * amount_needed
                    acc_amount_bought += amount_bought
                    amount_remain = amount_bought - amount_needed
                    print(f' --- --- Use { amount_needed = } at { price_bought = }, '
                          f'{ acc_amount_bought = }, { acc_total_bought = }')

                    if amount_remain > 0:
                        print(f' --- --- Remain { amount_remain = }')
                        heapq.heappush(max_heap, (-price_bought, amount_remain))

                profit = (amount*price) - acc_total_bought
                profits[asset] = profit
                print(f' --- { profit = }')

    profits = pd.DataFrame({'asset': profits.keys(), 'profit': profits.values()})
    return profits

def parse(tx_file):
    data = []
    with open(tx_file, 'r') as file:
        line = next(file)
        amount_idx = 0
        while line:
            reg_match = _RegExLib(line)
            if reg_match.market:
                action = reg_match.market.groupdict()['action']
                amount = reg_match.market.groupdict()['amount']
                asset = reg_match.market.groupdict()['asset']

            elif reg_match.quantity:
                if amount_idx == 0:
                    price = reg_match.quantity.groupdict()['amount']
                    price_unit = reg_match.quantity.groupdict()['unit']

                elif amount_idx == 1:
                    total = reg_match.quantity.groupdict()['amount']
                    total_unit = reg_match.quantity.groupdict()['unit']

                elif amount_idx == 2:
                    fee = reg_match.quantity.groupdict()['amount']
                    fee_unit = reg_match.quantity.groupdict()['unit']

                amount_idx = (amount_idx + 1) % 3

            elif reg_match.date:
                tx_datetime = datetime.strptime(reg_match.date.groupdict()['date'], "%d/%m/%y %I:%M %p")
                tx_record = {'action': action,
                             'amount': amount,
                             'asset': asset,
                             'price': price,
                             'price unit': price_unit,
                             'total': total,
                             'total unit': total_unit,
                             'fee': fee,
                             'fee unit': fee_unit,
                             'date': tx_datetime}
                data.append(tx_record)
            line = next(file, None)
        data = pd.DataFrame(data)

    return data

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="Transaction file")
    args = parser.parse_args()

    if args.file:
        tx_file = args.file
    else:
        tx_file = 'swyftx.usd.log'

    print(f'{ tx_file = }')
    data = parse(tx_file)
    profits = process(data)
    print(f'{ profits = }')
    print(f'{ profits["profit"].sum() = }')
