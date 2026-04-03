# AWS Deployment Files

This folder contains ready-to-use deployment files for hosting Intratrade on AWS EC2.

## Files
- `infra/aws/intratrade.service`: systemd service for Streamlit app
- `infra/aws/chandudevopai.shop.nginx.conf`: nginx reverse proxy config
- `infra/aws/iam-policy-s3.json`: IAM policy template for S3 access
- `infra/aws/deploy.sh`: one-shot setup script (Ubuntu)
- `infra/aws/deploy-amzn-linux-2023.sh`: one-shot setup script (Amazon Linux 2023)
- `web/streamlit/config.toml`: streamlit server config for reverse proxy mode

## Quick steps (manual)
1. Copy streamlit config:
   - `cp web/streamlit/config.toml .streamlit/config.toml`
2. Install service:
   - `sudo cp infra/aws/intratrade.service /etc/systemd/system/`
   - `sudo systemctl daemon-reload && sudo systemctl enable --now intratrade`
3. Install nginx vhost:
   - Ubuntu/Debian:
     - `sudo cp infra/aws/chandudevopai.shop.nginx.conf /etc/nginx/sites-available/chandudevopai.shop.conf`
     - `sudo ln -s /etc/nginx/sites-available/chandudevopai.shop.conf /etc/nginx/sites-enabled/`
   - Amazon Linux 2023:
     - `sudo cp infra/aws/chandudevopai.shop.nginx.conf /etc/nginx/conf.d/chandudevopai.shop.conf`
   - `sudo nginx -t && sudo systemctl reload nginx`
4. Issue SSL cert:
   - `sudo certbot --nginx -d chandudevopai.shop -d www.chandudevopai.shop`

## One-shot script (Ubuntu)
Update placeholders in `infra/aws/deploy.sh`:
- `REPLACE_GIT_REPO_URL`
- `REPLACE_EMAIL`

Run:
```bash
chmod +x infra/aws/deploy.sh
./infra/aws/deploy.sh
```

## One-shot script (Amazon Linux 2023)
Update placeholders in `infra/aws/deploy-amzn-linux-2023.sh`:
- `REPLACE_GIT_REPO_URL`
- `REPLACE_EMAIL`

Run:
```bash
chmod +x infra/aws/deploy-amzn-linux-2023.sh
./infra/aws/deploy-amzn-linux-2023.sh
```

