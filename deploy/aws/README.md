# AWS Deployment Files

This folder contains ready-to-use deployment files for hosting Intratrade on AWS EC2.

## Files
- `deploy/aws/intratrade.service`: systemd service for Streamlit app
- `deploy/aws/chandudevopai.shop.nginx.conf`: nginx reverse proxy config
- `deploy/aws/iam-policy-s3.json`: IAM policy template for S3 access
- `deploy/aws/deploy.sh`: one-shot setup script (Ubuntu)
- `deploy/aws/deploy-amzn-linux-2023.sh`: one-shot setup script (Amazon Linux 2023)
- `deploy/streamlit/config.toml`: streamlit server config for reverse proxy mode

## Quick steps (manual)
1. Copy streamlit config:
   - `cp deploy/streamlit/config.toml .streamlit/config.toml`
2. Install service:
   - `sudo cp deploy/aws/intratrade.service /etc/systemd/system/`
   - `sudo systemctl daemon-reload && sudo systemctl enable --now intratrade`
3. Install nginx vhost:
   - Ubuntu/Debian:
     - `sudo cp deploy/aws/chandudevopai.shop.nginx.conf /etc/nginx/sites-available/chandudevopai.shop.conf`
     - `sudo ln -s /etc/nginx/sites-available/chandudevopai.shop.conf /etc/nginx/sites-enabled/`
   - Amazon Linux 2023:
     - `sudo cp deploy/aws/chandudevopai.shop.nginx.conf /etc/nginx/conf.d/chandudevopai.shop.conf`
   - `sudo nginx -t && sudo systemctl reload nginx`
4. Issue SSL cert:
   - `sudo certbot --nginx -d chandudevopai.shop -d www.chandudevopai.shop`

## One-shot script (Ubuntu)
Update placeholders in `deploy/aws/deploy.sh`:
- `REPLACE_GIT_REPO_URL`
- `REPLACE_EMAIL`

Run:
```bash
chmod +x deploy/aws/deploy.sh
./deploy/aws/deploy.sh
```

## One-shot script (Amazon Linux 2023)
Update placeholders in `deploy/aws/deploy-amzn-linux-2023.sh`:
- `REPLACE_GIT_REPO_URL`
- `REPLACE_EMAIL`

Run:
```bash
chmod +x deploy/aws/deploy-amzn-linux-2023.sh
./deploy/aws/deploy-amzn-linux-2023.sh
```
