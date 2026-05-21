"""
清理用户已处理帖，用于重新抓取

用法:
  python clear_processed.py _pundao                    # 清空全部
  python clear_processed.py _pundao POST1 POST2 ...   # 只删指定帖
"""
import redis
from config import cfg
r = redis.Redis(host=cfg['redis_host'], port=cfg['redis_port'], password=cfg['redis_password'], db=cfg['redis_db'], decode_responses=True)

pids = 'DYMzLRmH-95 DX_tOlkn_K2 DX4I7e9Hz4H DXygOI9jq2b DXtG83sgWno DXocs_TFGy8 DXlcG9tjjUP DXjYqvvH5eX DW-trqXkRIK DW8IDWrDpgc DW37u-qEisC'.split()

for p in pids: 
    r.srem('instagram:_pundao:processed', p) or print(f'not found: {p}')

print(f'Remaining: {r.scard("instagram:_pundao:processed")}')

