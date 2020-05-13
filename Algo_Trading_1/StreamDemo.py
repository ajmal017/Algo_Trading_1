'''
Created on Apr 29, 2020

@author: aac75
'''
import os

import pandas as pd
from tqdm import tqdm
import websockets

import alpaca_trade_api as tradeapi
#import alpaca_conf_paper # loads API Keys as environment variables
import json
import asyncio


#api = tradeapi.REST(ALPACA_KEYID, ALPACA_SECRETKEY, ALPACA_STREAM_ENDPOINT, api_version='v2')


async def foo():
	ws = await websockets.connect("wss://api.alpaca.markets/stream")
	await ws.send(json.dumps({
		"action": "authenticate",
		"data": {
			"key_id": "AKYW8PFUO1E3BFM8MP90",
			"secret_key": "06izLiv7kQkRIpQ/alb2nszlU2RmFVWIzf6Mtbt8",
		}
	}))
	
	result = await ws.recv()
	
	return result
	
	

print(asyncio.run(foo()))

print('stuff')



#ws.send('{"action": "listen", "data": {"streams": ["T.SPY"]}}')
#result =  ws.recv()

#print(result)

