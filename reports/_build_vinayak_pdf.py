from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas

output = Path(r'F:\Trading\reports\vinayak_linux_deployment_guide.pdf')
output.parent.mkdir(parents=True, exist_ok=True)

lines = [
    'Vinayak Linux Deployment Guide',
    '',
    'Production Layout',
    '- App directory: /opt/vinayak',
    '- Service manager: systemd',
    '- Reverse proxy: nginx',
    '- App port: 8002',
    '- Public access: 80 / 443 via nginx',
    '',
    'Systemd Service File',
    '[Unit]',
    'Description=Vinayak API',
    'After=network.target',
    '',
    '[Service]',
    'Type=simple',
    'User=ubuntu',
    'Group=ubuntu',
    'WorkingDirectory=/opt/vinayak',
    'Environment="PYTHONUNBUFFERED=1"',
    'ExecStart=/usr/bin/python3 -m uvicorn vinayak.api.main:app --host 0.0.0.0 --port 8002',
    'Restart=always',
    'RestartSec=5',
    '',
    '[Install]',
    'WantedBy=multi-user.target',
    '',
    'Enable and Start',
    'sudo systemctl daemon-reload',
    'sudo systemctl enable vinayak',
    'sudo systemctl start vinayak',
    'sudo systemctl status vinayak',
    'journalctl -u vinayak -f',
    '',
    'Nginx Site Config',
    'server {',
    '    listen 80;',
    '    server_name _;',
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
    'Enable Nginx',
    'sudo ln -s /etc/nginx/sites-available/vinayak /etc/nginx/sites-enabled/vinayak',
    'sudo nginx -t',
    'sudo systemctl restart nginx',
    '',
    'Operations',
    '- Restart app: sudo systemctl restart vinayak',
    '- Restart nginx: sudo systemctl restart nginx',
    '- Check port: ss -lptn "sport = :8002"',
    '- Stop app: sudo systemctl stop vinayak',
    '',
    'HTTPS',
    'sudo apt update',
    'sudo apt install certbot python3-certbot-nginx -y',
    'sudo certbot --nginx',
    '',
    'Log Rotation',
    '/opt/vinayak/logs/*.log {',
    '    daily',
    '    rotate 14',
    '    compress',
    '    missingok',
    '    notifempty',
    '    copytruncate',
    '}',
    '',
    'Recommended Next Step',
    'Use systemd + nginx + env-file based secrets + health endpoint monitoring.',
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
pdf.setTitle('Vinayak Linux Deployment Guide')

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
