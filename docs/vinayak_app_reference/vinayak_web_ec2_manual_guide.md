# Vinayak Web EC2 Manual Guide

## Purpose

This guide covers the manual setup of the public Web EC2 in the 3-EC2 Vinayak topology.

## Responsibilities

The Web EC2 should run only:
- Nginx
- TLS certificates
- reverse proxy configuration

## Install Base Packages

Install:
- nginx
- certbot
- python3-certbot-nginx if using Certbot directly

## Nginx Proxy Target

The Web EC2 must proxy to the App EC2 private IP on port `8000`.

Example:

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://10.0.2.15:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

## Validation

Check:
- nginx syntax test passes
- App EC2 private IP responds through proxy
- browser can reach `/login`
- browser can reach `/admin`

## Security Group

Allow:
- 80 from internet
- 443 from internet
- 22 from admin IP only

Do not allow direct app or DB ports on this host.

## TLS

Enable HTTPS before public launch.

## Operations

- reload nginx after config changes
- keep domain and certificate renewal documented
- do not run app or database services on this host
