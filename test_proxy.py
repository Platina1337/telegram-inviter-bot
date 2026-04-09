
import os
import sys

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bot.config import Config

def test_proxy_parsing():
    c = Config()
    c.BOT_PROXY = "socks5://aN3kFWMy:MMuPqT42@154.212.30.248:64545"
    proxy_dict = c.get_bot_proxy_dict()
    print(f"Proxy dict: {proxy_dict}")

if __name__ == "__main__":
    test_proxy_parsing()
