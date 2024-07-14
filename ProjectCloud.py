import pandas as pd
import json
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.eventhub.exceptions import EventDataError
import asyncio
import yfinance as yf
connection_str = 'Endpoint=sb://cloudproject.servicebus.windows.net/;SharedAccessKeyName=PStockPrice;SharedAccessKey=1/m6wu88d8FjYpXfpQ9ReH1ef6FJM0t3w+AEhJgob7I=;EntityPath=projectstockprice'
eventhubs_name = 'projectstockprice'
def get_data_for_stock(stock):
    stock_pull = yf.Ticker(stock)
    stock_info = stock_pull.info
    stock_dataframe = pd.DataFrame([stock_info])[['symbol','open','currentPrice','marketCap','volume']]
    return stock_dataframe.to_dict("records")
get_data_for_stock('NVDA')
async def run():
    while True:
        await asyncio.sleep(10)
        producer = EventHubProducerClient.from_connection_string(
            conn_str = connection_str,
            eventhub_name = eventhubs_name
        )
        async with producer:
            event_data_batch = await producer.create_batch()
            event_data_batch.add(EventData(json.dumps(get_data_for_stock('NVDA'))))
            await producer.send_batch(event_data_batch)
            print('done')
loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(run())
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    print('Close')
    loop.close()
