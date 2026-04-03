from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas

output = Path(r'F:\Trading\reports\vinayak_ec2_ssl_ops_guide.pdf')
output.parent.mkdir(parents=True, exist_ok=True)

lines = [
    'Vinayak EC2 + SSL + Operations Guide',
    '',
    'Recommended Host Layout',
    '- OS: Ubuntu 22.04 or 24.04 LTS',
    '- App path: /opt/vinayak',
    '- Service user: ubuntu or dedicated vinayak user',
    '- App binds on localhost or 0.0.0.0:8002 depending on nginx layout',
    '- Public traffic handled by nginx on 80/443',
    '',
    'EC2 Security Group',
    '- Allow 22 from your IP only',
    '- Allow 80 from 0.0.0.0/0',
    '- Allow 443 from 0.0.0.0/0',
    '- Do not expose 8002 publicly unless absolutely required',
    '',
    'Initial Server Setup',
    'sudo apt update',
    'sudo apt install python3 python3-pip nginx -y',
    'sudo mkdir -p /opt/vinayak',
    'sudo chown -R ubuntu:ubuntu /opt/vinayak',
    '',
    'Suggested systemd Service with Env File',
    '[Unit]',
    'Description=Vinayak API',
    'After=network.target',
    '',
    '[Service]',
    'Type=simple',
    'User=ubuntu',
    'Group=ubuntu',
    'WorkingDirectory=/opt/vinayak',
    'EnvironmentFile=/etc/vinayak/vinayak.env',
    'Environment="PYTHONUNBUFFERED=1"',
    'ExecStart=/usr/bin/python3 -m uvicorn vinayak.api.main:app --host 127.0.0.1 --port 8002',
    'Restart=always',
    'RestartSec=5',
    '',
    '[Install]',
    'WantedBy=multi-user.target',
    '',
    'Example Env File: /etc/vinayak/vinayak.env',
    'VINAYAK_DATABASE_URL=sqlite:////opt/vinayak/data/vinayak.db',
    'VINAYAK_ADMIN_USERNAME=admin',
    'VINAYAK_ADMIN_PASSWORD=change-me',
    'TELEGRAM_BOT_TOKEN=your-token',
    'TELEGRAM_CHAT_ID=your-chat-id',
    '',
    'Lock Down the Env File',
    'sudo mkdir -p /etc/vinayak',
    'sudo nano /etc/vinayak/vinayak.env',
    'sudo chmod 600 /etc/vinayak/vinayak.env',
    '',
    'Nginx Domain Config',
    'server {',
    '    listen 80;',
    '    server_name your-domain.com www.your-domain.com;',
    '    location / {',
    '        proxy_pass http://127.0.0.1:8002;',
    '        proxy_http_version 1.1;',
    '        proxy_set_header Host $host;',
    '        proxy_set_header X-Real-IP $remote_addr;',
    '        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;',
    '        proxy_set_header X-Forwarded-Proto $scheme;',
    '    }',
    '}',
    '',
    'SSL with Certbot',
    'sudo apt install certbot python3-certbot-nginx -y',
    'sudo certbot --nginx -d your-domain.com -d www.your-domain.com',
    'sudo certbot renew --dry-run',
    '',
    'Deploy / Update Flow',
    '1. Pull latest code into /opt/vinayak',
    '2. Install dependencies if needed',
    '3. Run database migrations',
    '4. Restart vinayak service',
    '5. Check service and nginx health',
    '6. Confirm app and admin routes respond',
    '',
    'Restart Checklist',
    '- sudo systemctl restart vinayak',
    '- sudo systemctl status vinayak',
    '- journalctl -u vinayak -n 100 --no-pager',
    '- sudo nginx -t',
    '- sudo systemctl reload nginx',
    '- curl http://127.0.0.1:8002/health if available',
    '',
    'Backups',
    '- Back up the database before deploys',
    '- Back up env files and nginx config',
    '- Keep at least one rollback release copy',
    '',
    'Simple SQLite Backup Example',
    'mkdir -p /opt/vinayak/backups',
    'cp /opt/vinayak/data/vinayak.db /opt/vinayak/backups/vinayak_$(date +%F_%H-%M-%S).db',
    '',
    'Suggested Cron Backup',
    '0 */6 * * * cp /opt/vinayak/data/vinayak.db /opt/vinayak/backups/vinayak_$(date +\%F_\%H-\%M-\%S).db',
    '',
    'Post-Deploy Smoke Checks',
    '- Open the main app page',
    '- Open the admin page',
    '- Check reviewed trades API',
    '- Check executions API',
    '- Confirm paper execution logs are updating',
    '- Confirm Telegram notifications still send',
    '',
    'Operational Advice',
    '- Keep app on paper mode first after major changes',
    '- Monitor logs and outbox queue after restart',
    '- Prefer systemd restarts over manual nohup processes',
]

page_width, page_height = A4
left = 18 * mm
right = 18 * mm
usable_width = page_width - left - right
line_height = 6 * mm
font_name = 'Helvetica'
font_size = 10

def wrap(text: str):
    if not text:
        return ['']
    words = text.split(' ')
    out = []
    current = ''
    for word in words:
        trial = word if not current else current + ' ' + word
        if stringWidth(trial, font_name, font_size) <= usable_width:
            current = trial
        else:
            if current:
                out.append(current)
            current = word
    if current:
        out.append(current)
    return out

pdf = canvas.Canvas(str(output), pagesize=A4)
pdf.setTitle('Vinayak EC2 SSL Ops Guide')

x = left
y = page_height - 20 * mm
pdf.setFont(font_name, font_size)

for raw in lines:
    wrapped = wrap(raw)
    for line in wrapped:
        if y < 20 * mm:
            pdf.showPage()
            pdf.setFont(font_name, font_size)
            y = page_height - 20 * mm
        pdf.drawString(x, y, line)
        y -= line_height

pdf.save()
print(output)
