# 运行代理
go run server.go
go run client.go

# 测试 WebSocket
go run websocket_server.go
wscat -c ws://localhost:9000/ws


# 测试 HTTP
python3 -m http.server 8080
curl http://127.0.0.1:8080


# 测试 Mysql
docker run -d \
  --name mysql \
  -e MYSQL_ROOT_PASSWORD=123456 \
  -p 13306:3306 \
  mysql:8

pip install mycli
mysql -h 公网IP -P 13306 -uroot -p

mycli -h 39.105.15.153 -P 13306 -uroot -p