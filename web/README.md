# Web Tier

This folder holds the web-facing delivery assets for the 3-tier Vinayak layout.

- `nginx/`: reverse-proxy and ingress configs
- `streamlit/`: legacy UI server config kept for compatibility during migration

The Python page handlers still live under `app/vinayak/web/` because they are served by the application tier.
