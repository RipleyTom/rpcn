# RPCN

Server for matchmaking meant to be used with RPCS3(see PR for more information: https://github.com/RPCS3/rpcs3/pull/8663 ).

The server currently needs a certificate and its corresponding private key to be generated for it, you can use openssl for this:
```
openssl req -newkey rsa:4096 -new -nodes -x509 -days 3650 -keyout key.pem -out cert.pem
```
