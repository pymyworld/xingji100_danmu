# xingji100_danmu
主播数据平台弹幕实时收集，包括斗鱼、企鹅、熊猫、b站、全民、虎牙、龙珠、战旗

斗鱼、虎牙、龙珠、熊猫为node.js开发
* danmu_config.js为node.js弹幕收集脚本配置文件
* 斗鱼: 在 https://github.com/BacooTang/douyu-danmu 基础上进一步开发
* 虎牙: 在 https://github.com/BacooTang/huya-danmu 基础上进一步开发
* 龙珠: 在 https://github.com/BacooTang/longzhu-danmu 基础上进一步开发
* 熊猫: 在 https://github.com/BacooTang/panda-danmu 基础上进一步开发
* 在dbhelper.js对mysql的交互代码进行封装
需要安装的三方包有：
* douyu-danmu
* huya-danmu
* longzhu-danmu
* panda-danmu
* mongodb
* mysql
* log4js
* string-format

企鹅、b站、全民、战旗为python开发
* danmu_config.py为python弹幕收集脚本配置文件
* 全民、战旗: 在 https://github.com/littlecodersh/danmu 基础上进一步开发
* b站: 在 https://github.com/lyyyuna/bilibili_danmu 基础上进一步开发

弹幕收集结果全部存入mongodb中
存储格式为
{"anchor_id": 来源于xingji数据库, "platform_id": 来源于xingji数据库, "live_url": 来源于xingji数据库, "type": danmu or gift, "content": 礼物名或弹幕消息, "gift_num": 礼物数量}

