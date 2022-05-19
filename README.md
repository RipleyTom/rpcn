# RPCN

Server for matchmaking meant to be used with RPCS3.

All the settings and their descriptions are in rpcn.cfg.

RPCN needs a certificate and its corresponding private key to be generated for the encrypted TLS connections to clients,
you can use openssl for this:
```
openssl req -newkey rsa:4096 -new -nodes -x509 -days 3650 -keyout key.pem -out cert.pem
```

You can, optionally and disabled by default, generate a key to sign generated tickets:
```
openssl ecparam -name secp224k1 -genkey -noout -out ticket_private.pem
openssl ec -in ticket_private.pem -pubout -out ticket_public.pem
```
