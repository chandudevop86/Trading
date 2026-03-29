# 01-WEB-TIER

- The web tier/frontend tier of Vinayak serves browser traffic for the trading application.
- This tier exposes the pages `/admin` and `/workspace`.
- The preferred production entry is Nginx or AWS ALB.
- The web tier should forward application requests only to the app tier.

## Implement Web Tier

Install and configure the web entry layer:

```bash
sudo apt update
sudo apt install nginx -y
sudo systemctl enable nginx
sudo systemctl start nginx
```

Create a reverse proxy config:

```bash
sudo vi /etc/nginx/conf.d/vinayak.conf
```

Suggested config:

```nginx
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Reload Nginx:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

Verify in browser:

- `http://<server-ip>/admin`
- `http://<server-ip>/workspace`

Vinayak files used by this tier:

- `vinayak/web/app/main.py`
- `vinayak/web/app/workspace_html.py`
